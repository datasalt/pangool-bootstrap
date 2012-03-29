/**
 * Copyright [2012] [Datasalt Systems S.L.]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datasalt.pangool.bootstrap;

import java.io.IOException;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.datasalt.pangool.io.Fields;
import com.datasalt.pangool.io.ITuple;
import com.datasalt.pangool.io.Schema;
import com.datasalt.pangool.io.Tuple;
import com.datasalt.pangool.tuplemr.TupleMRBuilder;
import com.datasalt.pangool.tuplemr.TupleMRException;
import com.datasalt.pangool.tuplemr.TupleMapper;
import com.datasalt.pangool.tuplemr.TupleReducer;
import com.datasalt.pangool.tuplemr.mapred.lib.input.HadoopInputFormat;
import com.datasalt.pangool.tuplemr.mapred.lib.output.HadoopOutputFormat;

/**
 * Simple example job that writes the same records that reads
 * but sorted (input must be text)
 */
public class SortJob implements Tool, Configurable {

	Configuration conf;
	
	@Override
  public Configuration getConf() {
		return conf;
  }

	@Override
  public void setConf(Configuration conf) {
		this.conf = conf;
  }
	
	public static Schema getSchema() {
		return new Schema("line", Fields.parse("line:string"));
	}
		
	@SuppressWarnings("serial")
  public static class IdentityMap extends TupleMapper<LongWritable, Text> {
		
		Tuple tuple = new Tuple(getSchema());
		
		@Override
    public void map(LongWritable offset, Text line, TupleMRContext context,
        Collector collector) throws IOException, InterruptedException {
	    tuple.set("line", line.toString());
	    collector.write(tuple);
    }
		
	}
	
	@SuppressWarnings("serial")
  public static class IdentityReducer extends TupleReducer<Text, NullWritable> {

		public void reduce(ITuple group, Iterable<ITuple> tuples, TupleMRContext context, 
				Collector collector) throws IOException, InterruptedException, TupleMRException {
			
			for(ITuple tuple : tuples) {
				collector.write(new Text(tuple.get("line").toString()), NullWritable.get());
			}
		};
	}
	
	@Override
  public int run(String[] args) throws Exception {
		if(args.length != 2) {
			System.out.println("Invalid number of arguments\n\n" +
					"Usage: IdentityJob <input_path> <output_path>\n\n");
			return -1;
		}
		String input = args[0];
		String output = args[1];
		
		FileSystem.get(conf).delete(new Path(output), true);
		
		TupleMRBuilder mr = new TupleMRBuilder(conf);
		mr.addIntermediateSchema(getSchema());
		mr.setGroupByFields("line");
		mr.setTupleReducer(new IdentityReducer());
		mr.addInput(new Path(input), new HadoopInputFormat(TextInputFormat.class), new IdentityMap());
		mr.setOutput(new Path(output), new HadoopOutputFormat(TextOutputFormat.class), Text.class, NullWritable.class);
		mr.createJob().waitForCompletion(true);
	  return 0;
  }

	public static void main(String args[]) throws Exception {
		ToolRunner.run(new SortJob(), args);
	}
}
