#[macro_export]
#[cfg(feature = "profile")]
macro_rules! profile {
    ($name:expr, $($token:tt)+) => {
        {
            const LOG_THRESHOLD: std::time::Duration = std::time::Duration::from_micros(10);

            let _instant = std::time::Instant::now();
            let _result = {
                $($token)+
            };

            let dur = _instant.elapsed();
            if dur >= LOG_THRESHOLD {
                log::info!("[perf] {} {:?}", $name, dur);
            }

            _result
        }
    }
}

#[macro_export]
#[cfg(not(feature = "profile"))]
macro_rules! profile {
    ($name:expr, $($token:tt)+) => {
        $($token)+
    }
}
