package Config

import com.typesafe.config.{Config, ConfigFactory}

object configManager {

    var conf : Config = _

    def setup(fileName: String) {
        if (fileName == "")
            conf = ConfigFactory.load()
        conf = ConfigFactory.load(fileName)
    }

    def getString(key: String) : String = {
        conf.getString(key)
    }

    def getInt(key: String) : Int = {
        conf.getInt(key)
    }

    def getDouble(key: String) : Double = {
        conf.getDouble(key)
    }

    def getBoolean(key: String) : Boolean = {
        conf.getBoolean(key)
    }
}
