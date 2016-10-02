package app.yaml;


import java.util.Map;

public class YamlAppConfig {
    public String appName;
    public String defaultKeyspace;
    public String outputTable;
    public String outputKeyspace;
    public Integer minPartSize;
    public Map<String,String> sparkConfigs;
}
