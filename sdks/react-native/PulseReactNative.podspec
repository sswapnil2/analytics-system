require "json"

package = JSON.parse(File.read(File.join(__dir__, "package.json")))

Pod::Spec.new do |s|
  s.name         = "PulseReactNative"
  s.version      = package["version"]
  s.summary      = package["description"]
  s.homepage     = "https://github.com/sswapnil2/analytics-system"
  s.license      = package["license"]
  s.authors      = "Pulse Analytics"
  s.platforms    = { :ios => "13.4" }
  s.source       = { :git => "https://github.com/sswapnil2/analytics-system.git", :tag => "#{s.version}" }
  s.source_files = "ios/**/*.{h,m,mm,swift}"
  s.swift_version = "5.9"

  s.dependency "React-Core"
end
