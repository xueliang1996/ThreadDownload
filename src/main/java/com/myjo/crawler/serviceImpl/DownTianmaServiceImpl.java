package com.myjo.crawler.serviceImpl;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.http.NameValuePair;
import org.apache.http.client.CookieStore;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.BasicCookieStore;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.cookie.BasicClientCookie;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import com.csvreader.CsvReader;
import com.csvreader.CsvWriter;
import com.myjo.crawler.service.DownDataService;

/*
 * 下载天马导出的csv文件
 */
@Service
@EnableScheduling
public class DownTianmaServiceImpl implements DownDataService {

	@Autowired
	private AccessMethodService ams;

	@Value("${tianmaQuery}")
	private String tianmaQuery;
	private String time = null;
	private static final Logger LOGGER = LoggerFactory.getLogger(DownTianmaServiceImpl.class);
	// 用来保存数据
	private ArrayList<String[]> csvFileList = null;
	// 设置超时时间
	RequestConfig requestConfig = RequestConfig.custom().setConnectTimeout(100000).setConnectionRequestTimeout(100000)
			.setSocketTimeout(100000).build();
	// 启用多线程,线程池的大小根据任务变化
	private final static ExecutorService cachedThreadPool = Executors.newCachedThreadPool();

	/*
	 * 下载下的所有csv
	 * 
	 */
	@Scheduled(fixedDelayString = "${jobs.fixedDelay}")
	private void sendDownDataRequest() {
		LOGGER.info("正在发送请求并下载天马csv文件");
		time = String.valueOf(System.currentTimeMillis());
		JSONArray queryJson = new JSONArray(tianmaQuery);
		// System.out.println(queryJson.toString());

		FileReader fr = null;
		BufferedReader br = null;
		CloseableHttpClient httpClient = null;

		String result = null;
		String totalStr = null;
		String rootPath = System.getProperty("user.dir").replace("\\", "/");
		String filePath = rootPath + "/cookie.txt";
		System.out.println();
		try {
			fr = new FileReader(filePath);
			br = new BufferedReader(fr);
			// 读取cookie.txt中的cookie信息
			String cookies = br.readLine();
			String[] cookie = cookies.split("=");
			CookieStore cookieStore = new BasicCookieStore();
			BasicClientCookie bcCookie = new BasicClientCookie(cookie[0], cookie[1]);
			bcCookie.setVersion(0);
			bcCookie.setDomain("www.tianmasport.com");
			bcCookie.setPath("/ms");
			cookieStore.addCookie(bcCookie);
			httpClient = HttpClients.custom().setDefaultCookieStore(cookieStore).build();

			// 将form中的数据添加进去
			// 循环遍历需要查询的字段
			for (int i = 0; i < queryJson.length(); i++) {
				JSONObject json = queryJson.getJSONObject(i);
				String brand_name = json.getString("brand_name");
				String[] sexs = json.getString("sex").split(",");
				String[] divisions = json.getString("division").split(",");
				String[] quarters = json.getString("quarter").split(",");
				String[] seasons = json.getString("season").split(",");
				for (int m = 0; m < quarters.length; m++) {
					for (int n = 0; n < seasons.length; n++) {
						final int a = m;
						final int b = n;
						final CloseableHttpClient httpClients = httpClient;
						cachedThreadPool.execute(new Runnable() {

							@Override
							public void run() {
								try {
									// TODO Auto-generated method stub
									// 请求此url返回信息中total如果小于55000
									List<NameValuePair> form1 = new ArrayList<NameValuePair>();
									form1.add(new BasicNameValuePair("brand_name", brand_name));
									form1.add(new BasicNameValuePair("quarter", quarters[a] + seasons[b]));
									UrlEncodedFormEntity formEntity = new UrlEncodedFormEntity(form1, "utf-8");
									HttpPost total = new HttpPost(
											"http://www.tianmasport.com/ms/Inventory/groupList.do");
									total.setEntity(formEntity);
									total.setConfig(requestConfig);
									CloseableHttpResponse resps = httpClients.execute(total);
									String totalStr = EntityUtils.toString(resps.getEntity(), "utf-8");
									if (totalStr.contains("登录超时")) {
										LOGGER.info("登录超时!--重新执行登录");
										ams.sendRequestAndGetResponses();
										sendDownDataRequest();
									}
									JSONObject totalJson = new JSONObject(totalStr);
									if (totalJson.getInt("total") <= 50000) {
										// 请求这个地址可以拿到下载数据的链接需要拼接的Path
										HttpPost subForm = new HttpPost(
												"http://www.tianmasport.com/ms/Inventory/downGroup.do");
										subForm.setConfig(requestConfig);
										subForm.setEntity(formEntity);
										CloseableHttpResponse res = httpClients.execute(subForm);
										String result = EntityUtils.toString(res.getEntity(), "utf-8");
										// System.out.println(result);

										if (result.contains("没有可导出的信息") != true) {

											downDataTianMa(result, brand_name, "男", "鞋", quarters[a], seasons[b],
													httpClients);
										}
									} else {

										// 如果条数大于55000，则再次分类执行下载
										for (int j = 0; j < sexs.length; j++) {
											for (int k = 0; k < divisions.length; k++) {
												// System.out.println(brand_name);
												// System.out.println(sexs[j]);
												// System.out.println(divisions[k]);
												// System.out.println(quarters[m]
												// +
												// seasons[n]);

												List<NameValuePair> form2 = new ArrayList<NameValuePair>();
												form2.add(new BasicNameValuePair("brand_name", brand_name));
												form2.add(new BasicNameValuePair("sex", sexs[j]));
												form2.add(new BasicNameValuePair("division", divisions[k]));
												form2.add(new BasicNameValuePair("quarter", quarters[a] + seasons[b]));
												formEntity = new UrlEncodedFormEntity(form2, "utf-8");
												// 请求这个地址可以拿到下载数据的链接需要拼接的Path
												HttpPost subForms = new HttpPost(
														"http://www.tianmasport.com/ms/Inventory/downGroup.do");
												subForms.setEntity(formEntity);
												subForms.setConfig(requestConfig);
												CloseableHttpResponse resp = httpClients.execute(subForms);
												String result = EntityUtils.toString(resp.getEntity(), "utf-8");
												// System.out.println(result);
												if (result.contains("登录超时")) {
													LOGGER.info("登录超时!--重新执行登录");
													ams.sendRequestAndGetResponses();
													sendDownDataRequest();
												}
												if (result.contains("没有可导出的信息")) {
													continue;
												}
												downDataTianMa(result, brand_name, sexs[j], divisions[k], quarters[a],
														seasons[b], httpClients);
											}
										}
									}
								} catch (Exception e) {
									e.printStackTrace();
								}
							}
						});
					}
				}
				// 请求关闭、发生超时或者当前线程中断，无论哪一个首先发生之后，都将导致阻塞，直到所有任务完成执行
				// 设置最长等待10秒
				cachedThreadPool.awaitTermination(15, TimeUnit.MINUTES);
			}
			this.writeAllCsv();

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			LOGGER.error("下载CSV文件出错!");
			LOGGER.error("尝试重新下载---");
			try {
				Thread.sleep(1000 * 5);
			} catch (InterruptedException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			this.sendDownDataRequest();
		} finally {
			try {
				httpClient.close();
				fr.close();
				br.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}

	}

	/*
	 * 下载天马csv
	 */
	private void downDataTianMa(String result, String brand_name, String sex, String division, String quarter,
			String season, CloseableHttpClient httpClient) {
		InputStream csvData = null;
		FileOutputStream outputStream = null;
		try {

			JSONObject pathJosn = new JSONObject(result);
			String path = pathJosn.getString("path");
			HttpGet downDataUrl = new HttpGet("http://www.tianmasport.com/ms/dataDownLoad/downData.do?path=" + path);
			downDataUrl.setConfig(requestConfig);
			CloseableHttpResponse resps = httpClient.execute(downDataUrl);

			// 下载csv文件
			csvData = resps.getEntity().getContent();
			File file = new File("E:/MYJOProject/inventoryIAndMerge/outfile/outCSV" + "_" + brand_name + "_" + sex + "_"
					+ division + "_" + quarter + season + "_" + time + ".csv");
			outputStream = new FileOutputStream(file);
			byte[] buffer = new byte[1024];
			int len = 0;
			while ((len = csvData.read(buffer)) != -1) {
				outputStream.write(buffer, 0, len);
				outputStream.flush();
			}

			LOGGER.info("outCSV" + "_" + brand_name + "_" + sex + "_" + division + "_" + quarter + season + "_" + time
					+ ".csv下载成功");

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				outputStream.close();
				csvData.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

	}

	/*
	 * 合并csv文件
	 */

	private void writeAllCsv() {
		JSONArray queryJson = new JSONArray(tianmaQuery);
		// 定义一个CSV路径
		String csvFilePath = null;
		String mergeCSVPath = null;
		// 创建CSV读对象
		CsvReader reader = null;
		CsvWriter csvWriter = null;
		InputStreamReader isr = null;
		csvFileList = new ArrayList<String[]>();
		try {
			File files = new File("E:/MYJOProject/inventoryIAndMerge/mergeCSV/merge_Table" + "_" + time + ".csv");
			Boolean flag = files.createNewFile();
			LOGGER.info("创建合并后的csv文件成功否:" + flag);
			LOGGER.info("正在合并CSV文件");

			for (int i = 0; i < queryJson.length(); i++) {
				JSONObject json = queryJson.getJSONObject(i);
				String brand_name = json.getString("brand_name");
				String[] sexs = json.getString("sex").split(",");
				String[] divisions = json.getString("division").split(",");
				String[] quarters = json.getString("quarter").split(",");
				String[] seasons = json.getString("season").split(",");
				for (int j = 0; j < sexs.length; j++) {
					for (int k = 0; k < divisions.length; k++) {
						for (int m = 0; m < quarters.length; m++) {
							for (int n = 0; n < seasons.length; n++) {
								// System.out.println(brand_name);
								// System.out.println(sexs[j]);
								// System.out.println(divisions[k]);
								// System.out.println(quarters[m] + seasons[n]);

								csvFilePath = "E:/MYJOProject/inventoryIAndMerge/outfile/outCSV" + "_" + brand_name
										+ "_" + sexs[j] + "_" + divisions[k] + "_" + quarters[m] + seasons[n] + "_"
										+ time + ".csv";
								File f = new File(csvFilePath);
								if (f.exists() != true) {
									continue;
								}

								isr = new InputStreamReader(new FileInputStream(f), "GBK");
								reader = new CsvReader(isr);

								// 跳过表头 如果需要表头的话，这句可以忽略
								reader.readHeaders();
								// 逐行读入除表头的数据
								while (reader.readRecord()) {
									// System.out.println(reader.getRawRecord());
									csvFileList.add(reader.getValues());
								}
							}
						}
					}
				}
			}
			// 写到此路径下的CSV文件
			mergeCSVPath = "E:/MYJOProject/inventoryIAndMerge/mergeCSV/merge_Table" + "_" + time + ".csv";

			csvWriter = new CsvWriter(mergeCSVPath, ',', Charset.forName("GBK"));
			// 写头部
			String[] csvHeaders = { "商品货号", "货源", "中国尺码", "外国尺码", "品牌", "市场价", "库存数量", "类别", "小类", "性别", "季节", "折扣" };
			csvWriter.writeRecord(csvHeaders);
			for (int p = 0; p < csvFileList.size(); p++) {
				csvWriter.writeRecord(csvFileList.get(p));
			}
			LOGGER.info("合并CSV文件成功!");
		} catch (Exception e) {
			LOGGER.error("合并CSV文件出错!");
			e.printStackTrace();
		} finally {

			try {
				csvWriter.close();
				reader.close();
				isr.close();
				LOGGER.info("====================等待约15m=====================");
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}

	}

	@Override
	public String getFileSuffix() {
		return time;
	}

}
