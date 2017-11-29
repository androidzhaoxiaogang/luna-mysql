package luna.util;

import org.apache.commons.lang.builder.ToStringStyle;

import java.text.SimpleDateFormat;
import java.util.Date;

public class StringStyle {
    public static final ToStringStyle DEFAULT_STYLE    = new DateStyle("yyyy-MM-dd HH:mm:ss");

    private static class DateStyle extends ToStringStyle {
        private String            datePattern;
        public DateStyle(String datePattern){
            super();
            this.setUseIdentityHashCode(false);
            this.setUseShortClassName(true);
            this.datePattern = datePattern;
        }

        protected void appendDetail(StringBuffer buffer, String fieldName, Object value) {
            if (value instanceof Date) {
                value = new SimpleDateFormat(datePattern).format(value);
            } else {
                buffer.append(value);
            }
        }
    }
}
