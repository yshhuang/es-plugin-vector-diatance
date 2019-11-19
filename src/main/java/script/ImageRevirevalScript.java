package script;

import org.elasticsearch.script.ScoreScript;
import org.elasticsearch.search.lookup.SearchLookup;

import java.util.Map;

/**
 * @author yshhuang@foxmail.com
 * @date 2019-11-19 15:34
 */
public interface ImageRevirevalScript {
    ScoreScript.LeafFactory getScript(Map<String, Object> p,SearchLookup lookup);
}
