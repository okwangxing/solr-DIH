import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.apache.commons.lang.math.RandomUtils;

public class Review {
	public static class Split {
		public static String SPLIT = "_I_GUESS_I_AM_A_SPLITTER_";
		public static String SPLIT_LINE_END = "_I_GUESS_I_AM_A_LINE_END_SPLITTER_";
	}

	long id = 0;
	long biz_id = 0;
	long type = 0;
	long user_id = 0;
	long product_id = 0;
	long status = 0;
	long flag = 0;
	long supporter = 0;
	long objector = 0;
	long ip = 0;
	long create_time = 0;
	long replies = 0;
	long review_number = 0;
	long last_reply_time = 0;
	long edit_count = 0;
	long edit_last_time = 0;
	long edit_user_id = 0;
	String title = "";
	String content = "";

	@Override
	public String toString() {
		return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
	}

	public static Review convStr2Review(String reviewStr) {
		int titleBeginSplit = reviewStr.indexOf(Split.SPLIT);
		int titleEndSplit = reviewStr.lastIndexOf(Split.SPLIT);
		int lineEndSplit = reviewStr.lastIndexOf(Split.SPLIT_LINE_END);

		String numData = reviewStr.substring(0, titleBeginSplit);
		String title = reviewStr.substring(titleBeginSplit + Split.SPLIT.length(), titleEndSplit).trim();
		String content = reviewStr.substring(titleEndSplit + Split.SPLIT.length(), lineEndSplit).trim();

		String[] strlist = numData.trim().split("\t");

		if (strlist.length == 17) {
			Review d = new Review();
			d.id = Long.valueOf(strlist[0]) + RandomUtils.nextInt();
			d.biz_id = Long.valueOf(strlist[1]);
			d.type = Long.valueOf(strlist[2]);
			d.user_id = Long.valueOf(strlist[3]);
			d.product_id = Long.valueOf(strlist[4]);
			d.status = Long.valueOf(strlist[5]);
			d.flag = Long.valueOf(strlist[6]);
			d.supporter = Long.valueOf(strlist[7]);
			d.objector = Long.valueOf(strlist[8]);
			d.ip = Long.valueOf(strlist[9]);
			d.create_time = Long.valueOf(strlist[10]);
			d.replies = Long.valueOf(strlist[11]);
			d.review_number = Long.valueOf(strlist[12]);
			d.last_reply_time = Long.valueOf(strlist[13]);
			d.edit_count = Long.valueOf(strlist[14]);
			d.edit_last_time = Long.valueOf(strlist[15]);
			d.edit_user_id = Long.valueOf(strlist[16]);

			d.title = title;
			d.content = content;
			return d;
		}
		return null;
	}

}
