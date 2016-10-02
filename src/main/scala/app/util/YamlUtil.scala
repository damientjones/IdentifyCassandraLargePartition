package app.util

import java.io.FileReader

import app.yaml.YamlAppConfig
import com.esotericsoftware.yamlbeans.YamlReader
import scala.collection.JavaConverters._

object YamlUtil {
  private var config : YamlAppConfig = _
  private var configsSet : Boolean = _

  def parseYaml(fileName: String) {
    if (!configsSet) {
      val reader = new YamlReader(new FileReader(fileName))
      config = reader.read(classOf[YamlAppConfig])
      configsSet = true
    }
  }

  def getConfigs = config

  def getSparkConfigs : Map[String,String] = {
    config.sparkConfigs.asScala.toMap
  }
}
