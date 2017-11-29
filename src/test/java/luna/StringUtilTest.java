package luna;

import luna.util.StringUtil;

public class StringUtilTest{
    public static void main(String [] args){
        System.out.println(StringUtil.stripControl("2017-10-11 10:32:20.465"));
        System.out.println(StringUtil.stripEscape(StringUtil.stripControl("2017-10-11 10:32:20.465")));
        //System.out.println(StringUtil.stripControl(StringUtil.stripEscape("ssewew223344\u000essseegggffggr&*(&*……￥%##&……&%…………rwwweerr")));
        //System.out.println("\ufffe");
    }
}
