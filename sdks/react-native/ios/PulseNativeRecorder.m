#import <React/RCTBridgeModule.h>

@interface RCT_EXTERN_MODULE(PulseNativeRecorder, NSObject)

RCT_EXTERN_METHOD(captureWireframe:(NSDictionary *)options
                  resolver:(RCTPromiseResolveBlock)resolve
                  rejecter:(RCTPromiseRejectBlock)reject)

RCT_EXTERN__BLOCKING_SYNCHRONOUS_METHOD(randomBytesSync:(nonnull NSNumber *)length)

@end
