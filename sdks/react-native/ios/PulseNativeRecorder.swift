import Foundation
import Security
import UIKit

/// Native iOS wireframe recorder & CSPRNG bridge for `@pulse-analytics/react-native`.
///
/// Walks every visible `UIWindow` in the active scene (including `UISheetPresentationController`
/// bottom sheets, React Native `<Modal>` windows, and alerts), applies ancestor `clipsToBounds`
/// clipping rects, extracts real sRGB `bg_rgba` colors, and enforces default-deny PII masking.
@objc(PulseNativeRecorder)
public final class PulseNativeRecorder: NSObject {
  private static let fixedMask = "████"

  @objc
  public static func requiresMainQueueSetup() -> Bool {
    return false
  }

  /// Synchronous cryptographic random bytes backed by `SecRandomCopyBytes` (no `Math.random` on Hermes).
  @objc
  public func randomBytesSync(_ length: NSNumber) -> [NSNumber] {
    let count = max(1, min(64, length.intValue))
    var bytes = [UInt8](repeating: 0, count: count)
    let status = SecRandomCopyBytes(kSecRandomDefault, count, &bytes)
    if status != errSecSuccess {
      for i in 0..<count {
        bytes[i] = UInt8.random(in: 0...255)
      }
    }
    return bytes.map { NSNumber(value: $0) }
  }

  @objc
  public func captureWireframe(
    _ options: NSDictionary?,
    resolver resolve: @escaping RCTPromiseResolveBlock,
    rejecter reject: @escaping RCTPromiseRejectBlock
  ) {
    let maxNodes = max(16, min(500, (options?["maxNodes"] as? Int) ?? 160))
    DispatchQueue.main.async {
      let windows = Self.visibleWindows()
      guard let primary = windows.first else {
        resolve(["width": 0, "height": 0, "nodes": []])
        return
      }

      let screenBounds = primary.bounds
      let vpW = max(1, min(65535, Int(round(screenBounds.width))))
      let vpH = max(1, min(65535, Int(round(screenBounds.height))))

      var nodes: [[String: Any]] = []
      nodes.reserveCapacity(min(maxNodes, 128))
      nodes.append([
        "node_id": 1,
        "parent_id": 0,
        "kind": "screen",
        "x": 0,
        "y": 0,
        "w": vpW,
        "h": vpH,
        "bg_rgba": Self.rgbaUInt32(primary.backgroundColor, alphaScale: 1.0) ?? 0x090D16FF,
        "masked_label": Self.fixedMask,
      ])

      // Walk all visible windows bottom-to-top so modals, alerts, and bottom sheets layer accurately.
      for window in windows {
        if nodes.count >= maxNodes { break }
        let rootClip = window.bounds.intersection(screenBounds)
        if rootClip.isNull || rootClip.width < 2 || rootClip.height < 2 { continue }
        Self.walk(
          view: window,
          parentId: 1,
          clipRect: rootClip,
          inheritedMask: false,
          inheritedUnmask: false,
          maxNodes: maxNodes,
          nodes: &nodes
        )
      }

      resolve([
        "width": vpW,
        "height": vpH,
        "nodes": nodes,
      ])
    }
  }

  private static func visibleWindows() -> [UIWindow] {
    let connected = UIApplication.shared.connectedScenes
      .compactMap { $0 as? UIWindowScene }
      .filter { $0.activationState == .foregroundActive || $0.activationState == .foregroundInactive }
      .flatMap { $0.windows }
    let candidates = connected.isEmpty ? UIApplication.shared.windows : connected
    return candidates
      .filter { !$0.isHidden && $0.alpha > 0.01 && $0.bounds.width > 1 && $0.bounds.height > 1 }
      .sorted { $0.windowLevel.rawValue < $1.windowLevel.rawValue }
  }

  private static func stableNodeId(for view: UIView) -> UInt32 {
    let raw = UInt64(bitPattern: Int64(ObjectIdentifier(view).hashValue))
    let folded = UInt32((raw ^ (raw >> 32)) & 0xFFFF_FFFF)
    // Reserve 0 (none), 1 (screen), and 99_000 (error banner).
    return folded <= 1 || folded == 99_000 ? 2 : folded
  }

  private static func walk(
    view: UIView,
    parentId: UInt32,
    clipRect: CGRect,
    inheritedMask: Bool,
    inheritedUnmask: Bool,
    maxNodes: Int,
    nodes: inout [[String: Any]]
  ) {
    if nodes.count >= maxNodes { return }
    if view.isHidden || view.alpha <= 0.01 { return }

    let aid = view.accessibilityIdentifier ?? ""
    if aid.contains("pulse-block") { return }

    let forceMask = inheritedMask || aid.contains("pulse-mask")
    let allowUnmask = !forceMask && (inheritedUnmask || aid.contains("pulse-unmask"))

    let screenFrame = view.convert(view.bounds, to: nil)
    let visibleFrame = screenFrame.intersection(clipRect)
    if visibleFrame.isNull || visibleFrame.width < 4 || visibleFrame.height < 4 {
      return
    }

    let nextClip = view.clipsToBounds ? visibleFrame : clipRect
    let bgRgba = rgbaUInt32(view.backgroundColor, alphaScale: view.alpha)
    let nodeId = stableNodeId(for: view)
    var currentParentId = parentId

    if let (kind, label, color) = classify(
      view: view,
      bgRgba: bgRgba,
      allowUnmask: allowUnmask
    ) {
      nodes.append([
        "node_id": Int(nodeId),
        "parent_id": Int(parentId),
        "kind": kind,
        "x": max(0, min(65535, Int(round(visibleFrame.minX)))),
        "y": max(0, min(65535, Int(round(visibleFrame.minY)))),
        "w": max(1, min(65535, Int(round(visibleFrame.width)))),
        "h": max(1, min(65535, Int(round(visibleFrame.height)))),
        "bg_rgba": Int(color),
        "masked_label": label,
      ])
      currentParentId = nodeId
    }

    for subview in view.subviews {
      if nodes.count >= maxNodes { break }
      walk(
        view: subview,
        parentId: currentParentId,
        clipRect: nextClip,
        inheritedMask: forceMask,
        inheritedUnmask: allowUnmask,
        maxNodes: maxNodes,
        nodes: &nodes
      )
    }
  }

  private static func classify(
    view: UIView,
    bgRgba: UInt32?,
    allowUnmask: Bool
  ) -> (String, String, UInt32)? {
    if view is UITextField || view is UITextView {
      return ("masked_input", fixedMask, bgRgba ?? 0x1E293BFF)
    }
    if let btn = view as? UIButton {
      let raw = btn.currentTitle ?? btn.accessibilityLabel ?? ""
      return ("button", maskLabel(raw, allowUnmask: allowUnmask), bgRgba ?? 0x3B82F6FF)
    }
    if view.accessibilityTraits.contains(.button) {
      let raw = view.accessibilityLabel ?? ""
      return ("button", maskLabel(raw, allowUnmask: allowUnmask), bgRgba ?? 0x3B82F6FF)
    }
    if view is UIImageView {
      return ("image_placeholder", "media", bgRgba ?? 0x1F2937FF)
    }
    if let label = view as? UILabel {
      let raw = label.text ?? ""
      if raw.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty { return nil }
      return ("text", maskLabel(raw, allowUnmask: allowUnmask), bgRgba ?? 0x00000000)
    }
    // Emit colored containers (cards, bottom sheets, backdrops, headers) so layout & sheets render faithfully.
    if let color = bgRgba, (color & 0xFF) >= 8, !(view is UIWindow) {
      return ("container", "", color)
    }
    return nil
  }

  private static func maskLabel(_ raw: String, allowUnmask: Bool) -> String {
    let trimmed = raw.trimmingCharacters(in: .whitespacesAndNewlines)
    if trimmed.isEmpty { return "" }
    return allowUnmask ? String(trimmed.prefix(64)) : fixedMask
  }

  private static func rgbaUInt32(_ color: UIColor?, alphaScale: CGFloat) -> UInt32? {
    guard let color = color else { return nil }
    var r: CGFloat = 0
    var g: CGFloat = 0
    var b: CGFloat = 0
    var a: CGFloat = 0
    guard color.getRed(&r, green: &g, blue: &b, alpha: &a) else { return nil }
    let finalA = max(0, min(1, a * alphaScale))
    if finalA <= 0.01 { return nil }
    let ri = UInt32(round(max(0, min(1, r)) * 255))
    let gi = UInt32(round(max(0, min(1, g)) * 255))
    let bi = UInt32(round(max(0, min(1, b)) * 255))
    let ai = UInt32(round(finalA * 255))
    return (ri << 24) | (gi << 16) | (bi << 8) | ai
  }
}
