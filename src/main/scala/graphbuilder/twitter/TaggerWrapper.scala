package graphbuilder.twitter
import cmu.arktweetnlp.Tagger

object TaggerWrapper {
  val tagger = new Tagger  
  tagger.loadModel("model.20120919.20120919")
  def tokenizeAndTag (s : String) = tagger.tokenizeAndTag(s)
}