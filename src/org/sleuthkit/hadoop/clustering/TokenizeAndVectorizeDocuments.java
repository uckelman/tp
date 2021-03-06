/*
   Copyright 2011 Basis Technology Corp.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
 */

package org.sleuthkit.hadoop.clustering;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.vectorizer.DefaultAnalyzer;
import org.apache.mahout.vectorizer.DictionaryVectorizer;
import org.apache.mahout.vectorizer.DocumentProcessor;
import org.apache.mahout.vectorizer.tfidf.TFIDFConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.sleuthkit.hadoop.core.SKJobFactory;

/** Contains methods allowing one to
 *  tokenize and then vectorize documents previously exported to
 *  sequence files. This class uses mahout to generate TF and TF-IDF
 *  vectors for the documents on a specific disk. 
 */
public class TokenizeAndVectorizeDocuments {

    private static final Logger log = LoggerFactory.getLogger(TokenizeAndVectorizeDocuments.class);

    private static final String DIR_TEXT_DEFAULT = "/texaspete/text";
    private static final String DIR_TOKENIZED_DEFAULT = "/texaspete/tokens";
    private static final String DIR_VECTOR_DEFAULT = "/texaspete/vectors";

    public TokenizeAndVectorizeDocuments() {}

    public static void main (String[] argv) throws Exception {
        runPipeline(DIR_TEXT_DEFAULT, DIR_TOKENIZED_DEFAULT, DIR_VECTOR_DEFAULT);
    }

    public static int runPipeline(String textdir, String tokendir, String vectordir) {
        // Placeholder. This will convert a sample file into the sequencefile we so desire.
        Path input;
        Path output;
        
        Configuration cfg = new Configuration();
        try {
            SKJobFactory.addDependencies(cfg);
            
        } catch (IOException ex) {
            log.error("Exception while adding dependencies.", ex);
        }

        // Assume we already have a ID:Text sequencefile directory.
        // We now proceed to run the DocumentProcessor class, which will turn
        // the SequenceFile into a ID:StringTuple file.
        input = new Path(textdir);
        output = new Path(tokendir);

        try {
            DocumentProcessor.tokenizeDocuments(input, DefaultAnalyzer.class, output, cfg);
        } catch(Exception ex) {
            log.error("Error tokenizing documents", ex);
            return 1;
        }

        // We are now going to take the SequenceFile we got from above and
        // convert it to vectors using DictionaryVectorizer. This should create
        // a SequenceFile of ID:Vector. This is the final processing step we
        // need to do; after this we have our vector list to work off of.
        input = output;
        output = new Path(vectordir);

        int minSupport = 2;
        int maxNGramSize = 1;
        float minLLRValue = 1;
        float normPower = -1.0f; // using this disables normalization.
        boolean logNormalize = false;
        int numReducers = 1;
        int chunkSizeInMegabytes = 200;
        boolean sequentialAccess = false;
        boolean namedVectors = true;

        

        try {
            DictionaryVectorizer.createTermFrequencyVectors(input,
                    output,
                    cfg,
                    minSupport,
                    maxNGramSize,
                    minLLRValue,
                    normPower,
                    logNormalize,
                    numReducers,
                    chunkSizeInMegabytes,
                    sequentialAccess,
                    namedVectors);
        } catch (Exception ex) {
            log.error("Error creating TF vectors for documents", ex);
            return 1;
        }

        // Generate TF-IDF vectors from the TF vectors. These give better
        // results for clustering.

        input = new Path(vectordir + "/tf-vectors");
        output = new Path(vectordir);

        // On large datasets, these may make a big difference by getting rid
        // of very rare, distinctive features and overly common, insignificant
        // features.
        int minDocumentFrequency = 5;
        int maxDocumentFrequencyPercent = 40;

        try {
            TFIDFConverter.processTfIdf(
                    input,
                    output,
                    cfg,
                    chunkSizeInMegabytes,
                    minDocumentFrequency,
                    maxDocumentFrequencyPercent,
                    normPower,
                    logNormalize,
                    sequentialAccess,
                    namedVectors,
                    numReducers);
        } catch (Exception ex) {
            log.error("Error creating TF-IDF Vectors from TF-vectors", ex);
            return 1;
        }

        return 0;
    }
}
