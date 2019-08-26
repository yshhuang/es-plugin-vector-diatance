import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.ScriptPlugin;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptEngine;

import java.util.Collection;

/**
 * @author yshhuang@foxmail.com
 * @date 2019-08-22 15:29
 */
public class VectorDistancePlugin extends Plugin implements ScriptPlugin {
    @Override
    public ScriptEngine getScriptEngine(Settings settings,Collection<ScriptContext<?>> contexts) {
        return new VectorDistanceEngine();
    }
}
