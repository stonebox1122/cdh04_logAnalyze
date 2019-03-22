package com.stone;

public class ETLUtil {
	
	public static String etlStr(String line) {
		
		// 1 过滤不符合规则的数据
		String[] split = line.split("\t");
		if (split.length < 9) {
			return null;
		}
		
		// 2 去除类别字段中空格
		split[3] = split[3].replaceAll(" ", "");
		
		// 3 替换关联视频的分隔符
//		String relativeIds = "";
//		String[] newStr = new String[10];
//		if (split.length >= 11) {
//			for (int i = 9; i < split.length; i++) {
//				relativeIds += split[i] + "&";
//			}
//			split[9] = relativeIds.substring(0, relativeIds.length()-1);
//			System.arraycopy(split, 0, newStr, 0, 10);
//			return StringUtils.join(newStr,",");
//		}
//		return StringUtils.join(split,",");
		
		StringBuffer sb = new StringBuffer();
		for (int i = 0; i < split.length; i++) {
			if (i < 9) {
				if (i == split.length - 1) {
					sb.append(split[i]);
				} else {
					sb.append(split[i]).append("\t");
				}
			} else {
				if (i == split.length - 1) {
					sb.append(split[i]);
				} else {
					sb.append(split[i]).append("&");
				}
			}
		}
		return sb.toString();
	}

}
