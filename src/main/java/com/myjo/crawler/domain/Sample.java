package com.myjo.crawler.domain;

@Component
public class Sample {
	@Value("${baidu.APP_ID}")
	private static String APP_ID;
	@Value("${baidu.API_KEY}")
	private static String API_KEY;
	@Value("${baidu.SECRET_KEY}")
	private static String SECRET_KEY;

	public static String getAppId() {
		return APP_ID;
	}

	public static String getApiKey() {
		return API_KEY;
	}

	public static String getSecretKey() {
		return SECRET_KEY;
	}
}

