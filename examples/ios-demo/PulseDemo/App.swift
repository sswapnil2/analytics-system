import UIKit

// MARK: - Pulse setup

enum Pulse {
    static let client: PulseClient = {
        let dir = FileManager.default.urls(for: .applicationSupportDirectory, in: .userDomainMask)[0]
            .appendingPathComponent("pulse").path
        let defaults = UserDefaults.standard // launch with: -pulseKey pk_live_... -pulseEndpoint http://...
        return try! PulseClient(config: SdkConfig(
            endpointUrl: defaults.string(forKey: "pulseEndpoint") ?? "http://127.0.0.1:18190",
            apiKey: defaults.string(forKey: "pulseKey") ?? "pk_live_REPLACE_ME",
            storageDir: dir,
            flushBatchSize: 20,
            flushIntervalMs: 3000
        ))
    }()

    static func track(_ name: String, _ props: [String: Any] = [:]) {
        let json = String(data: try! JSONSerialization.data(withJSONObject: props), encoding: .utf8)!
        try? client.track(eventName: name, propsJson: json)
    }

    // The SDK has no view-tree capture on iOS: we walk UIKit ourselves and hand it boxes.
    static func snapshot(_ window: UIWindow) {
        var nodes: [SdkWireNode] = []
        let size = window.bounds.size
        nodes.append(SdkWireNode(nodeId: 1, parentId: 0, kind: "screen", x: 0, y: 0,
                                 w: UInt16(size.width), h: UInt16(size.height), bgRgba: 0xFFFFFFFF,
                                 label: "Screen", unmasked: true))
        func walk(_ v: UIView) {
            guard !v.isHidden, v.alpha > 0.01 else { return }
            let r = v.convert(v.bounds, to: window)
            var kind: String?
            var label = ""
            var unmasked = true
            switch v {
            case let f as UITextField: kind = "masked_input"; label = f.text ?? ""; unmasked = false
            case let b as UIButton: kind = "button"; label = b.configuration?.title ?? b.currentTitle ?? ""
            case let l as UILabel where l.tag == 99: kind = "error_banner"; label = l.text ?? ""
            case let l as UILabel: kind = "text"; label = l.text ?? ""
            default: break
            }
            if let kind, r.width >= 4, r.height >= 4, r.minX >= 0, r.minY >= 0 {
                nodes.append(SdkWireNode(nodeId: UInt32(nodes.count + 1), parentId: 1, kind: kind,
                                         x: UInt16(r.minX), y: UInt16(r.minY), w: UInt16(r.width), h: UInt16(r.height),
                                         bgRgba: kind == "button" ? 0x3B82F6FF : 0xF1F5F9FF,
                                         label: label, unmasked: unmasked))
                if kind == "button" || kind == "masked_input" { return }
            }
            v.subviews.forEach(walk)
        }
        walk(window)
        try? client.recordWireframeSnapshot(width: UInt16(size.width), height: UInt16(size.height), nodes: nodes)
    }
}

// Every touch goes through here so the SDK can record taps / rage taps / dead taps.
final class PulseWindow: UIWindow {
    override func sendEvent(_ event: UIEvent) {
        if let t = event.allTouches?.first, t.phase == .ended {
            let p = t.location(in: self)
            let interactive = t.view is UIControl
            _ = try? Pulse.client.recordTouch(x: UInt16(max(0, p.x)), y: UInt16(max(0, p.y)),
                                             targetIsInteractive: interactive,
                                             tsMs: Int64(Date().timeIntervalSince1970 * 1000))
        }
        super.sendEvent(event)
    }
}

// MARK: - App

@main
final class AppDelegate: UIResponder, UIApplicationDelegate {
    var window: UIWindow?

    func application(_ app: UIApplication, didFinishLaunchingWithOptions _: [UIApplication.LaunchOptionsKey: Any]?) -> Bool {
        try? Pulse.client.identify(userId: "ios_demo_user_1")
        try? Pulse.client.setUserProperties(propsJson: #"{"platform":"ios","plan":"free"}"#)
        let w = PulseWindow(frame: UIScreen.main.bounds)
        w.rootViewController = UINavigationController(rootViewController: StepVC(step: 0))
        w.makeKeyAndVisible()
        window = w
        return true
    }
}

final class StepVC: UIViewController {
    let step: Int
    static let screens = ["welcome", "profile", "plan", "done"]
    init(step: Int) { self.step = step; super.init(nibName: nil, bundle: nil) }
    required init?(coder: NSCoder) { fatalError() }

    let stack = UIStackView()
    let errorLabel = UILabel()

    override func viewDidLoad() {
        super.viewDidLoad()
        view.backgroundColor = .systemBackground
        stack.axis = .vertical
        stack.spacing = 16
        stack.translatesAutoresizingMaskIntoConstraints = false
        view.addSubview(stack)
        NSLayoutConstraint.activate([
            stack.topAnchor.constraint(equalTo: view.safeAreaLayoutGuide.topAnchor, constant: 24),
            stack.leadingAnchor.constraint(equalTo: view.leadingAnchor, constant: 24),
            stack.trailingAnchor.constraint(equalTo: view.trailingAnchor, constant: -24),
        ])
        switch step {
        case 0:
            title("Welcome to DemoPay"); text("Open an account in three steps.")
            button("Get started") { Pulse.track("Onboarding Started"); self.next() }
        case 1:
            title("Your details")
            field("Full name", "Asha Rao"); field("PAN number", "ABCDE1234F")
            button("Continue") { Pulse.track("Profile Submitted", ["has_pan": true]); self.next() }
        case 2:
            title("Pick a plan")
            button("Apply promo code") { self.fail() }  // deliberately broken: rage-tap me
            errorLabel.tag = 99; errorLabel.textColor = .systemRed; errorLabel.isHidden = true
            stack.addArrangedSubview(errorLabel)
            button("Choose Pro") { Pulse.track("Plan Selected", ["plan": "pro"]); self.next() }
        default:
            title("You're all set")
            Pulse.track("Onboarding Completed")
        }
    }

    override func viewDidAppear(_ animated: Bool) {
        super.viewDidAppear(animated)
        Pulse.track("Onboarding Screen Viewed", ["screen": Self.screens[step]])
        if let w = view.window { Pulse.snapshot(w) }
    }

    func next() { navigationController?.pushViewController(StepVC(step: step + 1), animated: true) }

    func fail() {
        let msg = "PromoService unavailable"
        errorLabel.text = msg
        errorLabel.isHidden = false
        Pulse.track("$error", ["message": msg])
        view.layoutIfNeeded()
        if let w = view.window { Pulse.snapshot(w) }
    }

    func title(_ s: String) { let l = UILabel(); l.text = s; l.font = .boldSystemFont(ofSize: 28); stack.addArrangedSubview(l) }
    func text(_ s: String) { let l = UILabel(); l.text = s; stack.addArrangedSubview(l) }
    func field(_ placeholder: String, _ value: String) {
        let f = UITextField(); f.placeholder = placeholder; f.text = value; f.borderStyle = .roundedRect
        stack.addArrangedSubview(f)
    }
    func button(_ s: String, _ action: @escaping () -> Void) {
        let b = UIButton(configuration: .filled(), primaryAction: UIAction(title: s) { _ in action() })
        stack.addArrangedSubview(b)
    }
}
