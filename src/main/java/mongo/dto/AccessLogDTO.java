package mongo.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Getter;
import lombok.Setter;

@JsonInclude(JsonInclude.Include.NON_DEFAULT)
@Getter
@Setter
public class AccessLogDTO {

    private String ip;
    private String reqTime;
    private String reqMethod;
    private String reqURI;
}
