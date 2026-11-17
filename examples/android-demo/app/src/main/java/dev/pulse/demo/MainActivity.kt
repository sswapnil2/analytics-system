package dev.pulse.demo

import android.app.Activity
import android.graphics.Color
import android.os.Bundle
import android.view.MotionEvent
import android.view.View
import android.view.ViewGroup
import android.widget.Button
import android.widget.EditText
import android.widget.LinearLayout
import android.widget.TextView
import org.json.JSONObject
import uniffi.pulse_sdk_core.PulseClient
import uniffi.pulse_sdk_core.SdkConfig
import uniffi.pulse_sdk_core.SdkWireNode

object Pulse {
    lateinit var client: PulseClient

    fun init(activity: Activity) {
        if (::client.isInitialized) return
        client = PulseClient(SdkConfig(
            endpointUrl = BuildConfig.PULSE_ENDPOINT,
            apiKey = BuildConfig.PULSE_KEY,
            storageDir = activity.filesDir.resolve("pulse").path,
            flushBatchSize = 20u,
            flushIntervalMs = 3000uL,
        ))
    }

    fun track(name: String, props: Map<String, Any> = emptyMap()) =
        client.track(name, JSONObject(props).toString())

    // No view-tree capture in the SDK on Android either: walk the hierarchy ourselves.
    fun snapshot(root: View) {
        val nodes = mutableListOf(SdkWireNode(1u, 0u, "screen", 0u, 0u, root.width.toUShort(), root.height.toUShort(),
            0xFFFFFFFFu, "Screen", true))
        val loc = IntArray(2)
        fun walk(v: View) {
            if (v.visibility != View.VISIBLE) return
            v.getLocationInWindow(loc)
            val (kind, label, unmasked) = when (v) {
                is EditText -> Triple("masked_input", v.text.toString(), false)
                is Button -> Triple("button", v.text.toString(), true)
                is TextView -> Triple(if (v.tag == "error") "error_banner" else "text", v.text.toString(), true)
                else -> Triple(null, "", true)
            }
            if (kind != null && v.width >= 4 && v.height >= 4) {
                nodes += SdkWireNode((nodes.size + 1).toUInt(), 1u, kind, loc[0].toUShort(), loc[1].toUShort(),
                    v.width.toUShort(), v.height.toUShort(), if (kind == "button") 0x3B82F6FFu else 0xF1F5F9FFu,
                    label, unmasked)
            }
            if (v is ViewGroup) for (i in 0 until v.childCount) walk(v.getChildAt(i))
        }
        walk(root)
        client.recordWireframeSnapshot(root.width.toUShort(), root.height.toUShort(), nodes)
    }
}

class MainActivity : Activity() {
    private var step = 0
    private val screens = listOf("welcome", "profile", "plan", "done")

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        Pulse.init(this)
        Pulse.client.identify("android_demo_user_1")
        Pulse.client.setUserProperties("""{"platform":"android","plan":"free"}""")
        render()
    }

    // Every tap goes through the SDK so it can detect rage / dead taps.
    override fun dispatchTouchEvent(ev: MotionEvent): Boolean {
        if (ev.action == MotionEvent.ACTION_UP) {
            val hit = findHit(window.decorView, ev.rawX.toInt(), ev.rawY.toInt())
            Pulse.client.recordTouch(ev.x.toInt().toUShort(), ev.y.toInt().toUShort(), hit?.isClickable == true,
                System.currentTimeMillis())
        }
        return super.dispatchTouchEvent(ev)
    }

    private fun findHit(v: View, x: Int, y: Int): View? {
        val loc = IntArray(2).also { v.getLocationOnScreen(it) }
        if (x < loc[0] || y < loc[1] || x > loc[0] + v.width || y > loc[1] + v.height) return null
        if (v is ViewGroup) for (i in v.childCount - 1 downTo 0) findHit(v.getChildAt(i), x, y)?.let { return it }
        return v
    }

    private fun render() {
        val col = LinearLayout(this).apply { orientation = LinearLayout.VERTICAL; setPadding(48, 96, 48, 48) }
        fun text(s: String, size: Float = 16f) = col.addView(TextView(this).apply { text = s; textSize = size })
        fun field(hint: String, value: String) = col.addView(EditText(this).apply { this.hint = hint; setText(value) })
        fun button(s: String, onClick: () -> Unit) = col.addView(Button(this).apply { text = s; setOnClickListener { onClick() } })
        when (step) {
            0 -> { text("Welcome to DemoPay", 28f); text("Open an account in three steps.")
                   button("Get started") { Pulse.track("Onboarding Started"); next() } }
            1 -> { text("Your details", 28f); field("Full name", "Asha Rao"); field("PAN number", "ABCDE1234F")
                   button("Continue") { Pulse.track("Profile Submitted", mapOf("has_pan" to true)); next() } }
            2 -> { text("Pick a plan", 28f)
                   val err = TextView(this).apply { tag = "error"; setTextColor(Color.RED); visibility = View.GONE }
                   button("Apply promo code") { // deliberately broken: rage-tap me
                       err.text = "PromoService unavailable"; err.visibility = View.VISIBLE
                       Pulse.track("\$error", mapOf("message" to err.text.toString()))
                       col.post { Pulse.snapshot(window.decorView) }
                   }
                   col.addView(err)
                   button("Choose Pro") { Pulse.track("Plan Selected", mapOf("plan" to "pro")); next() } }
            else -> { text("You're all set", 28f); Pulse.track("Onboarding Completed") }
        }
        setContentView(col)
        Pulse.track("Onboarding Screen Viewed", mapOf("screen" to screens[step]))
        col.post { Pulse.snapshot(window.decorView) }
    }

    private fun next() { step++; render() }
}
