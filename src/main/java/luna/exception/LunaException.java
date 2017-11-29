package luna.exception;

import org.apache.commons.lang.exception.NestableRuntimeException;

public class LunaException extends NestableRuntimeException {

    public LunaException(String errorCode){
        super(errorCode);
    }

    public LunaException(String errorCode, Throwable cause){
        super(errorCode, cause);
    }

    public LunaException(String errorCode, String errorDesc){
        super(errorCode + ":" + errorDesc);
    }

    public LunaException(String errorCode, String errorDesc, Throwable cause){
        super(errorCode + ":" + errorDesc, cause);
    }

    public LunaException(Throwable cause){
        super(cause);
    }
}
