package graphbuilder.twitter
import cmu.arktweetnlp.Tagger
import java.net.URL

object TaggerWrapper {
  val tagger = new Tagger
  tagger.loadModel("/model.20120919.20120919")
  def tokenizeAndTag (s : String) = tagger.tokenizeAndTag(s)
}