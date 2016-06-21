//package org.apache.hadoop.examples;
import com.google.common.collect.ObjectArrays;
import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Arrays;
import org.apache.commons.lang.ArrayUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import java.util.Set;
import java.lang.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.LongWritable;
import java.util.HashSet;

import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class UniqueWords {

  public static class TokenizerMapper 
       extends Mapper<Object, Text, Text, IntWritable>{
    
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
/*      
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
	String wd="";
      while (itr.hasMoreTokens()) {
	wd=itr.nextToken().toLowerCase().trim();
	if(Character.isLetter(wd.charAt(0)) || !Character.isDigit(wd.charAt(0))|| wd.contains(" "))
	{
	wd=removePunctuation(wd); //remove punctuation
                wd=removeStopWord(wd);
	wd=removeCharacters(wd);
        word.set(wd);
        context.write(word, one);
	}
      }
    }
*/
private static final Pattern PATTERN = Pattern.compile("\\w+");

public static class PorterStemmer
{  private char[] b;
   private int i,     /* offset into b */
               i_end, /* offset to end of stemmed word */
               j, k;
   private static final int INC = 50;
                     /* unit of size whereby b is increased */
   public PorterStemmer()
   {  b = new char[INC];
      i = 0;
      i_end = 0;
   }
 

   public void add(char ch)
   {  if (i == b.length)
      {  char[] new_b = new char[i+INC];
         for (int c = 0; c < i; c++) new_b[c] = b[c];
         b = new_b;
      }
      b[i++] = ch;
   }


 

   public void add(char[] w, int wLen)
   {  if (i+wLen >= b.length)
      {  char[] new_b = new char[i+wLen+INC];
         for (int c = 0; c < i; c++) new_b[c] = b[c];
         b = new_b;
      }
      for (int c = 0; c < wLen; c++) b[i++] = w[c];
   }
 
   public String toString() { return new String(b,0,i_end); }
 
   public int getResultLength() { return i_end; }

 
   public char[] getResultBuffer() { return b; }
 

   private final boolean cons(int i)
   {  switch (b[i])
      {  case 'a': case 'e': case 'i': case 'o': case 'u': return false;
         case 'y': return (i==0) ? true : !cons(i-1);
         default: return true;
      }
   }

 

   private final int m()
   {  int n = 0;
      int i = 0;
      while(true)
      {  if (i > j) return n;
         if (! cons(i)) break; i++;
      }
      i++;
      while(true)
      {  while(true)
         {  if (i > j) return n;
               if (cons(i)) break;
               i++;
         }
         i++;
         n++;
         while(true)
         {  if (i > j) return n;
            if (! cons(i)) break;
            i++;
         }
         i++;
       }
   }

    

   private final boolean vowelinstem()
   {  int i; for (i = 0; i <= j; i++) if (! cons(i)) return true;
      return false;
   }

    

   private final boolean doublec(int j)
   {  if (j < 1) return false;
      if (b[j] != b[j-1]) return false;
      return cons(j);
   }

 

   private final boolean cvc(int i)
   {  if (i < 2 || !cons(i) || cons(i-1) || !cons(i-2)) return false;
      {  int ch = b[i];
         if (ch == 'w' || ch == 'x' || ch == 'y') return false;
      }
      return true;
   }

   private final boolean ends(String s)
   {  int l = s.length();
      int o = k-l+1;
      if (o < 0) return false;
      for (int i = 0; i < l; i++) if (b[o+i] != s.charAt(i)) return false;
      j = k-l;
      return true;
   }

   
   private final void setto(String s)
   {  int l = s.length();
      int o = j+1;
      for (int i = 0; i < l; i++) b[o+i] = s.charAt(i);
      k = j+l;
   }

    

   private final void r(String s) { if (m() > 0) setto(s); }

   
   private final void step1()
   {  if (b[k] == 's')
      {  if (ends("sses")) k -= 2; else
         if (ends("ies")) setto("i"); else
         if (b[k-1] != 's') k--;
      }
      if (ends("eed")) { if (m() > 0) k--; } else
      if ((ends("ed") || ends("ing")) && vowelinstem())
      {  k = j;
         if (ends("at")) setto("ate"); else
         if (ends("bl")) setto("ble"); else
         if (ends("iz")) setto("ize"); else
         if (doublec(k))
         {  k--;
            {  int ch = b[k];
               if (ch == 'l' || ch == 's' || ch == 'z') k++;
            }
         }
         else if (m() == 1 && cvc(k)) setto("e");
     }
   }

   /* step2() turns terminal y to i when there is another vowel in the stem. */

   private final void step2() { if (ends("y") && vowelinstem()) b[k] = 'i'; }

    

   private final void step3() { if (k == 0) return; /* For Bug 1 */ switch (b[k-1])
   {
       case 'a': if (ends("ational")) { r("ate"); break; }
                 if (ends("tional")) { r("tion"); break; }
                 break;
       case 'c': if (ends("enci")) { r("ence"); break; }
                 if (ends("anci")) { r("ance"); break; }
                 break;
       case 'e': if (ends("izer")) { r("ize"); break; }
                 break;
       case 'l': if (ends("bli")) { r("ble"); break; }
                 if (ends("alli")) { r("al"); break; }
                 if (ends("entli")) { r("ent"); break; }
                 if (ends("eli")) { r("e"); break; }
                 if (ends("ousli")) { r("ous"); break; }
                 break;
       case 'o': if (ends("ization")) { r("ize"); break; }
                 //if (ends("ation")) { r("ate"); break; }
                 if (ends("ator")) { r("ate"); break; }
                 break;
       case 's': if (ends("alism")) { r("al"); break; }
                 if (ends("iveness")) { r("ive"); break; }
                 if (ends("fulness")) { r("ful"); break; }
                 if (ends("ousness")) { r("ous"); break; }
                 break;
       case 't': if (ends("aliti")) { r("al"); break; }
                 if (ends("iviti")) { r("ive"); break; }
                 if (ends("biliti")) { r("ble"); break; }
                 break;
       case 'g': if (ends("logi")) { r("log"); break; }
   } }

   

   private final void step4() { switch (b[k])
   {
       case 'e': if (ends("icate")) { r("ic"); break; }
                 if (ends("ative")) { r(""); break; }
                 if (ends("alize")) { r("al"); break; }
                 break;
       case 'i': if (ends("iciti")) { r("ic"); break; }
                 break;
       case 'l': if (ends("ical")) { r("ic"); break; }
                 if (ends("ful")) { r(""); break; }
                 break;
       case 's': if (ends("ness")) { r(""); break; }
                 break;
   } }

    

   private final void step5()
   {   if (k == 0) return; /* for Bug 1 */ switch (b[k-1])
       {  case 'a': if (ends("al")) break; return;
          case 'c': if (ends("ance")) break;
                    if (ends("ence")) break; return;
          case 'e': if (ends("er")) break; return;
          case 'i': if (ends("ic")) break; return;
          case 'l': if (ends("able")) break;
                    if (ends("ible")) break; return;
          case 'n': if (ends("ant")) break;
                    if (ends("ement")) break;
                    if (ends("ment")) break;
                    /* element etc. not stripped before the m */
                    if (ends("ent")) break; return;
          case 'o': //if (ends("ion") && j >= 0 && (b[j] == 's' || b[j] == 't')) break;
                                    /* j >= 0 fixes Bug 2 */
                    if (ends("ou")) break; return;
                    /* takes care of -ous */
          case 's': if (ends("ism")) break; return;
          case 't': if (ends("ate")) break;
                    if (ends("iti")) break; return;
          case 'u': if (ends("ous")) break; return;
          case 'v': if (ends("ive")) break; return;
          case 'z': if (ends("ize")) break; return;
          default: return;
       }
       if (m() > 1) k = j;
   }

    
   private final void step6()
   {  j = k;
      if (b[k] == 'e')
      {  int a = m();
         if (a > 1 || a == 1 && !cvc(k-1)) k--;
      }
      if (b[k] == 'l' && doublec(k) && m() > 1) k--;
   }

   
   public void stem()
   {  k = i - 1;
      if (k > 1) { step1(); //step2(); step3(); step4(); step5(); step6(); 
		}
      i_end = k+1; i = 0;
   }
   }
public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {

 Matcher m = PATTERN.matcher(value.toString());

            // Get the name of the file from the input-split in the context
            String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();

            // build the values and write <k,v> pairs through the context
            StringBuilder valueBuilder = new StringBuilder();
			PorterStemmer s = new PorterStemmer();
            while (m.find()) {
                String matchedKey = m.group().toLowerCase();
                // remove names starting with non letters, digits, considered stopwords or containing other chars
             if (!Character.isLetter(matchedKey.charAt(0)) || Character.isDigit(matchedKey.charAt(0))|| matchedKey.contains("_") || matchedKey.length() < 3){
		 //`if( Character.isDigit(matchedKey.charAt(0)) || !Character.isLetter(matchedKey.charAt(0)))
		// {
                //if ( 
                  //       matchedKey.contains("_") || matchedKey.length() < 3) {
                    continue;
                }
		//matchedKey=removePunctuation(matchedKey);
		matchedKey=removeStopWord(matchedKey);
		matchedKey=removeCharacters(matchedKey);
		//if(matchedKey != "0")
		//{	
		for (int c = 0; c < matchedKey.length(); c++) s.add(matchedKey.charAt(c));
                                s.stem();
                                String u;
                                u = s.toString();
			u=removeStopWord(u);
                u=removeCharacters(u);
		if(!u.equals("0") && u.length() >=3)
		{
		valueBuilder.append(u);
		this.word.set(u);
                //valueBuilder.append("@");
                //valueBuilder.append(fileName);
                // emit the partial <k,v>
                context.write(this.word, one);
                valueBuilder.setLength(0);
		//}
		}	
            }
        }
    }

 



  
  public static class IntSumReducer 
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, 
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 2) {
      System.err.println("Usage: wordcount <in> <out>");
      System.exit(2);
    }
    Job job = new Job(conf, "word count");
    job.setJarByClass(UniqueWords.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }

public static String removeStopWord(String str){


String[] ENGLISH_STOP_WORDS1 ={"able","about","above","abroad","according","accordingly","across","actually","adj","after","afterwards","again","against","ago","ahead","ain't","all","allow","allows","almost","alone","along","alongside","already","also","although","always","am","amid","amidst","among","amongst","an","and","another","any","anybody","anyhow","anyone","anything","anyway","anyways","anywhere","apart","appear","appreciate","appropriate","are","aren't","around","as","a's","aside","ask","asking","associated","at","available","away","awfully","back","backward","backwards","be","became","because","become","becomes","becoming","been","before","beforehand","begin","behind","being","believe","below","beside","besides","best","better","between","beyond","both","brief","but","by","came","can","cannot","cant","can't","caption","cause","causes","certain","certainly","changes","clearly","c'mon","co","co.","com","come","comes","concerning","consequently","consider","considering","contain","containing","contains","corresponding","could","couldn't","course","c's","currently","dare","daren't","definitely","described","despite","did","didn't","different","directly","do","does","doesn't","doing","done","don't","down","downwards","during","each","edu","eg","eight","eighty","either","else","elsewhere","end","ending","enough","entirely","especially","et","etc","even","ever","evermore","every","everybody","everyone","everything","everywhere","ex","exactly","example","except","fairly","far","farther","few","fewer","fifth","first","five","followed","following","follows","for","forever","former","formerly","forth","forward","found","four","from","further","furthermore","get","gets","getting","given","gives","go","goes","going","gone","got","gotten","greetings","had","hadn't","half","happens","hardly","has","hasn't","have","haven't","having","he","he'd","he'll","hello","help","hence","her","here","hereafter","hereby","herein","here's","hereupon","hers","herself","he's","hi","him","himself","his","hither","hopefully","how","howbeit","however","hundred","i'd","ie","if","ignored","i'll","i'm","immediate","in","inasmuch","inc","inc.","indeed","indicate","indicated","indicates","inner","inside","insofar","instead","into","inward","is","isn't","it","it'd","it'll","its","it's","itself","i've","just","k","keep","keeps","kept","know","known","knows","last","lately","later","latter","latterly","least","less","lest","let","let's","like","liked","likely","likewise","little","look","looking","looks","low","lower","ltd","made","mainly","make","makes","many","may","maybe","mayn't","me","mean","meantime","meanwhile","merely","might","mightn't","mine","minus","miss","more","moreover","most","mostly","mr","mrs","much","must","mustn't","my","myself","name","namely","nd","near","nearly","necessary","need","needn't","needs","neither","never","neverf","neverless","nevertheless","new","next","nine","ninety","no","nobody","non","none","nonetheless","noone","no-one","nor"};

String[] ENGLISH_STOP_WORDS2={"normally","not","nothing","notwithstanding","novel","now","nowhere","obviously","of","off","often","oh","ok","okay","old","on","once","one","ones","one's","only","onto","opposite","or","other","others","otherwise","ought","oughtn't","our","ours","ourselves","out","outside","over","overall","own","particular","particularly","past","per","perhaps","placed","please","plus","possible","presumably","probably","provided","provides","que","quite","qv","rather","rd","re","really","reasonably","recent","recently","regarding","regardless","regards","relatively","respectively","right","round","said","same","saw","say","saying","says","second","secondly","see","seeing","seem","seemed","seeming","seems","seen","self","selves","sensible","sent","serious","seriously","seven","several","shall","shan't","she","she'd","she'll","she's","should","shouldn't","since","six","so","some","somebody","someday","somehow","someone","something","sometime","sometimes","somewhat","somewhere","soon","sorry","specified","specify","specifying","still","sub","such","sup","sure","take","taken","taking","tell","tends","th","than","thank","thanks","thanx","that","that'll","thats","that's","that've","the","their","theirs","them","themselves","then","thence","there","thereafter","thereby","there'd","therefore","therein","there'll","there're","theres","there's","thereupon","there've","these","they","they'd","they'll","they're","they've","thing","things","think","third","thirty","this","thorough","thoroughly","those","though","three","through","throughout","thru","thus","till","to","together","too","took","toward","towards","tried","tries","truly","try","trying","t's","twice","two","un","under","underneath","undoing","unfortunately","unless","unlike","unlikely","until","unto","up","upon","upwards","us","use","used","useful","uses","using","usually","v","value","various","versus","very","via","viz","vs","want","wants","was","wasn't","way","we","we'd","welcome","well","we'll","went","were","we're","weren't","we've","what","whatever","what'll","what's","what've","when","whence","whenever","where","whereafter","whereas","whereby","wherein","where's","whereupon","wherever","whether","which","whichever","while","whilst","whither","who","who'd","whoever","whole","who'll","whom","whomever","who's","whose","why","will","willing","wish","with","within","without","wonder","won't","would","wouldn't","yes","yet","you","you'd","you'll","your","you're","yours","yourself","yourselves","you've","zero","xxxxx10x","xxx","thou","thee","thy","thine","ye","macb","rom","jul","enter","lady","prince","king","gutenberg"};

String[] ENGLISH_STOP_WORDS=combine(ENGLISH_STOP_WORDS1,ENGLISH_STOP_WORDS2);

for (int i=0; i<ENGLISH_STOP_WORDS.length; i++){
if (str.equals(ENGLISH_STOP_WORDS[i])){
//str.replace(ENGLISH_STOP_WORDS[i],"");
str="0";
}
}
return str;
}

public static String[] combine(String[] a, String[] b){
        int length = a.length + b.length;
        String[] result = new String[length];
        System.arraycopy(a, 0, result, 0, a.length);
        System.arraycopy(b, 0, result, a.length, b.length);
        return result;
    }


public static String removeCharacters(String str){
String[] ENGLISH_CHARACTERS_WORDS = { "duncan","malcolm","donalbain","macbeth","lady macbeth","banquo","fleance","macduff","lady macduff","macduff's son","ross","lennox","angus","menteith","caithness","siward","young siward","seyton","hecate","three witches","captain","three murderers","two murderers","porter","doctor","doctor","gentlewoman","lord","first apparition","second apparition","third apparition","attendants","messengers","servants","soldiers","friar laurence","friar john","apothecary","chorus","abram","balthasar","benvolio","romeo","montague","lady montague","capulet","gregory","sampson","peter","rosaline","juliet","nurse","lady capulet","matriarch","tybalt","mercutio","prince escalus","verona","escalus","kinsman","thou","thee","thy","thine","ye","macb","rom","jul","enter","lady","prince","king","gutenberg"
};
for (int i=0; i<ENGLISH_CHARACTERS_WORDS.length; i++){
if (str.equals(ENGLISH_CHARACTERS_WORDS[i])){
str="0";
}
}
return str;
}


public static String removePunctuation(String str){
int len=0;
int i=0;
char lastChar=0;
String Ch="ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
//remove prefix
while(i<3){
str=str.trim();
len=str.length();
if (len==0) break;
lastChar=str.charAt(0);
if(Ch.indexOf(Character.toString(lastChar))==-1){
str=str.substring(1,len);
}else{
break;
}
}

while(i<3){
str=str.trim();
len=str.length();
if(len==0) break;
lastChar=str.charAt(len-1);
if(Ch.indexOf(Character.toString(lastChar))==-1){
str=str.substring(0,len-1);
}else{
break;
}
}
return str.toLowerCase(); //return word in lowercase
}

}
