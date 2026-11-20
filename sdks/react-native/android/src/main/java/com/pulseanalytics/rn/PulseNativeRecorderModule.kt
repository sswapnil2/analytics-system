package com.pulseanalytics.rn

import android.graphics.Rect
import android.graphics.drawable.ColorDrawable
import android.graphics.drawable.GradientDrawable
import android.os.Handler
import android.os.Looper
import android.view.View
import android.view.ViewGroup
import android.widget.Button
import android.widget.EditText
import android.widget.ImageView
import android.widget.TextView
import com.facebook.react.bridge.Arguments
import com.facebook.react.bridge.Promise
import com.facebook.react.bridge.ReactApplicationContext
import com.facebook.react.bridge.ReactContextBaseJavaModule
import com.facebook.react.bridge.ReactMethod
import com.facebook.react.bridge.ReadableMap
import com.facebook.react.bridge.WritableArray
import java.security.SecureRandom
import kotlin.math.max
import kotlin.math.min
import kotlin.math.roundToInt

/**
 * Native Android wireframe recorder & CSPRNG bridge for `@pulse-analytics/react-native`.
 *
 * Walks all attached root windows (including `BottomSheetDialog`, `AlertDialog`, React Native
 * `ModalHostView` dialogs, and `PopupWindow` overlays), intersects ancestor clip bounds, extracts
 * actual background colors (`ColorDrawable`, `GradientDrawable`, `ReactViewBackgroundDrawable`),
 * and enforces default-deny PII masking.
 */
class PulseNativeRecorderModule(
    private val reactContext: ReactApplicationContext,
) : ReactContextBaseJavaModule(reactContext) {

    private val secureRandom = SecureRandom()
    private val mainHandler = Handler(Looper.getMainLooper())

    override fun getName(): String = "PulseNativeRecorder"

    /** Synchronous cryptographic random bytes backed by `SecureRandom` (no `Math.random` on Hermes). */
    @ReactMethod(isBlockingSynchronousMethod = true)
    fun randomBytesSync(length: Int): WritableArray {
        val count = length.coerceIn(1, 64)
        val bytes = ByteArray(count)
        secureRandom.nextBytes(bytes)
        val out = Arguments.createArray()
        for (b in bytes) {
            out.pushInt(b.toInt() and 0xFF)
        }
        return out
    }

    @ReactMethod
    fun captureWireframe(options: ReadableMap?, promise: Promise) {
        val maxNodes = if (options != null && options.hasKey("maxNodes")) {
            options.getInt("maxNodes").coerceIn(16, 500)
        } else {
            160
        }

        mainHandler.post {
            try {
                val roots = collectWindowRoots()
                if (roots.isEmpty()) {
                    val empty = Arguments.createMap().apply {
                        putInt("width", 0)
                        putInt("height", 0)
                        putArray("nodes", Arguments.createArray())
                    }
                    promise.resolve(empty)
                    return@post
                }

                val density = reactContext.resources.displayMetrics.density.coerceAtLeast(1f)
                val primary = roots.first()
                val vpW = (primary.width / density).roundToInt().coerceIn(1, 65535)
                val vpH = (primary.height / density).roundToInt().coerceIn(1, 65535)
                val screenRectPx = Rect(0, 0, primary.width, primary.height)

                val nodes = Arguments.createArray()
                var count = 0

                val screenNode = Arguments.createMap().apply {
                    putInt("node_id", 1)
                    putInt("parent_id", 0)
                    putString("kind", "screen")
                    putInt("x", 0)
                    putInt("y", 0)
                    putInt("w", vpW)
                    putInt("h", vpH)
                    putDouble("bg_rgba", (extractBgRgba(primary, 1f) ?: 0x090D16FFL).toDouble())
                    putString("masked_label", FIXED_MASK)
                }
                nodes.pushMap(screenNode)
                count += 1

                for (root in roots) {
                    if (count >= maxNodes) break
                    if (root.visibility != View.VISIBLE || root.alpha <= 0.01f) continue
                    count = walk(
                        view = root,
                        parentId = 1,
                        clipRectPx = screenRectPx,
                        density = density,
                        inheritedMask = false,
                        inheritedUnmask = false,
                        maxNodes = maxNodes,
                        currentCount = count,
                        nodes = nodes,
                    )
                }

                val result = Arguments.createMap().apply {
                    putInt("width", vpW)
                    putInt("height", vpH)
                    putArray("nodes", nodes)
                }
                promise.resolve(result)
            } catch (t: Throwable) {
                promise.reject("PULSE_CAPTURE_ERR", t.message, t)
            }
        }
    }

    /**
     * Returns all attached window root views in z-order (Activity decor view + BottomSheetDialogs +
     * React Native `<Modal>` windows), falling back cleanly to `currentActivity.window.decorView`.
     */
    @Suppress("UNCHECKED_CAST")
    private fun collectWindowRoots(): List<View> {
        try {
            val wmgClass = Class.forName("android.view.WindowManagerGlobal")
            val wmg = wmgClass.getMethod("getInstance").invoke(null)
            val viewsField = wmgClass.getDeclaredField("mViews").apply { isAccessible = true }
            val raw = viewsField.get(wmg)
            val list = when (raw) {
                is List<*> -> raw.filterIsInstance<View>()
                is Array<*> -> raw.filterIsInstance<View>()
                else -> emptyList()
            }
            val visible = list.filter { it.isAttachedToWindow && it.visibility == View.VISIBLE && it.width > 0 && it.height > 0 }
            if (visible.isNotEmpty()) return visible
        } catch (_: Throwable) {
            // Fall back to current Activity decorView if reflection is restricted.
        }
        val decor = reactContext.currentActivity?.window?.decorView
        return if (decor != null) listOf(decor) else emptyList()
    }

    private fun stableNodeId(view: View): Int {
        val raw = System.identityHashCode(view) and 0x7FFF_FFFF
        return if (raw <= 1 || raw == 99_000) 2 else raw
    }

    private fun walk(
        view: View,
        parentId: Int,
        clipRectPx: Rect,
        density: Float,
        inheritedMask: Boolean,
        inheritedUnmask: Boolean,
        maxNodes: Int,
        currentCount: Int,
        nodes: WritableArray,
    ): Int {
        var count = currentCount
        if (count >= maxNodes) return count
        if (view.visibility != View.VISIBLE || view.alpha <= 0.01f) return count

        val marker = "${view.contentDescription ?: ""} ${view.tag ?: ""}"
        if (marker.contains("pulse-block")) return count

        val forceMask = inheritedMask || marker.contains("pulse-mask")
        val allowUnmask = !forceMask && (inheritedUnmask || marker.contains("pulse-unmask"))

        val loc = IntArray(2)
        view.getLocationOnScreen(loc)
        val viewRect = Rect(loc[0], loc[1], loc[0] + view.width, loc[1] + view.height)
        val visible = Rect(viewRect)
        if (!visible.intersect(clipRectPx)) return count

        val xDp = (visible.left / density).roundToInt().coerceIn(0, 65535)
        val yDp = (visible.top / density).roundToInt().coerceIn(0, 65535)
        val wDp = (visible.width() / density).roundToInt()
        val hDp = (visible.height() / density).roundToInt()
        if (wDp < 4 || hDp < 4) return count

        val bgRgba = extractBgRgba(view, view.alpha)
        val nodeId = stableNodeId(view)
        var nextParentId = parentId

        val classified = classify(view, bgRgba, allowUnmask)
        if (classified != null) {
            val (kind, label, color) = classified
            val map = Arguments.createMap().apply {
                putInt("node_id", nodeId)
                putInt("parent_id", parentId)
                putString("kind", kind)
                putInt("x", xDp)
                putInt("y", yDp)
                putInt("w", wDp.coerceIn(1, 65535))
                putInt("h", hDp.coerceIn(1, 65535))
                putDouble("bg_rgba", color.toDouble())
                putString("masked_label", label)
            }
            nodes.pushMap(map)
            count += 1
            nextParentId = nodeId
        }

        if (view is ViewGroup) {
            val nextClip = if (view.clipChildren || view.clipToPadding) visible else clipRectPx
            for (i in 0 until view.childCount) {
                if (count >= maxNodes) break
                val child = view.getChildAt(i) ?: continue
                count = walk(
                    view = child,
                    parentId = nextParentId,
                    clipRectPx = nextClip,
                    density = density,
                    inheritedMask = forceMask,
                    inheritedUnmask = allowUnmask,
                    maxNodes = maxNodes,
                    currentCount = count,
                    nodes = nodes,
                )
            }
        }
        return count
    }

    private fun classify(
        view: View,
        bgRgba: Long?,
        allowUnmask: Boolean,
    ): Triple<String, String, Long>? {
        if (view is EditText) {
            return Triple("masked_input", FIXED_MASK, bgRgba ?: 0x1E293BFFL)
        }
        if (view is Button || (view.isClickable && view !is ViewGroup && view !is ImageView)) {
            val raw = (view as? TextView)?.text?.toString() ?: view.contentDescription?.toString() ?: ""
            return Triple("button", maskLabel(raw, allowUnmask), bgRgba ?: 0x3B82F6FFL)
        }
        if (view is ImageView) {
            return Triple("image_placeholder", "media", bgRgba ?: 0x1F2937FFL)
        }
        if (view is TextView) {
            val raw = view.text?.toString() ?: ""
            if (raw.trim().isEmpty()) return null
            return Triple("text", maskLabel(raw, allowUnmask), bgRgba ?: 0x00000000L)
        }
        if (bgRgba != null && (bgRgba and 0xFFL) >= 8L && view.parent != null) {
            return Triple("container", "", bgRgba)
        }
        return null
    }

    private fun maskLabel(raw: String, allowUnmask: Boolean): String {
        val trimmed = raw.trim()
        if (trimmed.isEmpty()) return ""
        return if (allowUnmask) trimmed.take(64) else FIXED_MASK
    }

    private fun extractBgRgba(view: View, alphaScale: Float): Long? {
        val bg = view.background ?: return null
        val argb: Int? = when (bg) {
            is ColorDrawable -> bg.color
            is GradientDrawable -> bg.color?.defaultColor
            else -> try {
                // Works for React Native's `ReactViewBackgroundDrawable` (`getColor(): Int`).
                val method = bg.javaClass.getMethod("getColor")
                method.invoke(bg) as? Int
            } catch (_: Throwable) {
                null
            }
        }
        if (argb == null) return null
        val a = ((argb ushr 24) and 0xFF)
        val scaledA = (a * alphaScale).roundToInt().coerceIn(0, 255)
        if (scaledA <= 2) return null
        val r = (argb ushr 16) and 0xFF
        val g = (argb ushr 8) and 0xFF
        val b = argb and 0xFF
        return (r.toLong() shl 24) or (g.toLong() shl 16) or (b.toLong() shl 8) or scaledA.toLong()
    }

    companion object {
        private const val FIXED_MASK = "████"
    }
}
