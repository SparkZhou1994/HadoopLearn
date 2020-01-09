package spark.hadoop.project;

import com.kumkee.userAgent.UserAgent;
import com.kumkee.userAgent.UserAgentParser;
import org.apache.commons.lang.StringUtils;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @ClassName UserAgentTest
 * @Description TODO
 * @Author Spark
 * @Date 8/16/2019 12:41 PM
 * @Version 1.0
 **/
public class UserAgentTest {

    @Test
    public void testUserAgentParser(){
        String source = "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3664.3 Safari/537.36";

        UserAgentParser userAgentParser = new UserAgentParser();
        UserAgent agent =userAgentParser.parse(source);

        String browser = agent.getBrowser();
        String engine = agent.getEngine();
        String engineVersion = agent.getEngineVersion();
        String os = agent.getOs();
        String platform = agent.getPlatform();
        Boolean isMobile = agent.isMobile();
        System.out.println(browser + "," + engine + "," + engineVersion + "," + os + "," + platform + "," + isMobile);
    }

    @Test
    public void testReadFile() throws Exception{
        String path = "E://Hadoop Operation Document.txt";

        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(new File(path))));
        String line= "";
        Integer i = 0;
        Map<String, Integer> browserMap = new HashMap<String, Integer>();
        UserAgentParser userAgentParser = new UserAgentParser();
        while(line != null) {
            line = reader.readLine();
            i ++;
            if (StringUtils.isNotBlank(line)) {
                String source = line.substring(getCharacterPosition(line, "\"", 7)) + 1;
                UserAgent agent = userAgentParser.parse(source);
                String browser = agent.getBrowser();
                String engine = agent.getEngine();
                String engineVersion = agent.getEngineVersion();
                String os = agent.getOs();
                String platform = agent.getPlatform();
                Boolean isMobile = agent.isMobile();

                Integer browserValue = browserMap.get(browser);
                if (browserValue != null) {
                    browserMap.put(browser,browserValue + 1);
                } else {
                    browserMap.put(browser, 1);
                }
                System.out.println(browser + "," + engine + "," + engineVersion + "," + os + "," + platform + "," + isMobile);
            }
        }
        System.out.println("UserAgentTest.testReadFile, record count: " + i);
        for (Map.Entry<String, Integer> entry : browserMap.entrySet()) {
            System.out.println(entry.getKey() + " : " + entry.getValue());
        }
    }

    @Test
    public void testGetCharacterPosition() {
        String value = "192.168.4.128 - - [Friday] \" \"  \" \"  \" \" \"Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3664.3 Safari/537.36\"";
        Integer index = getCharacterPosition(value, "\"", 7);
        System.out.println(index);
    }

    private Integer getCharacterPosition(String value, String operator, Integer index) {
        Matcher slashMatcher = Pattern.compile(operator).matcher(value);
        Integer mIdx = 0;
        while(slashMatcher.find()) {
            mIdx ++;
            if (mIdx == index) {
                break;
            }
        }
        return slashMatcher.start();
    }
}
