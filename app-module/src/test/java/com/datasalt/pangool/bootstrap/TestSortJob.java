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

import java.io.File;
import java.nio.charset.Charset;

import org.apache.hadoop.util.ToolRunner;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import com.datasalt.pangool.utils.test.AbstractHadoopTestLibrary;
import com.google.common.io.Files;

/**
 * Test class for {@link SortJob}
 */
public class TestSortJob extends AbstractHadoopTestLibrary {

	private final static String INPUT = "test-input-" + TestSortJob.class.getName();
  private final static String OUTPUT = "test-output-" + TestSortJob.class.getName();
  
  @Test
  public void test() throws Exception {
  	trash(OUTPUT);

  	File iFile = new File(INPUT);
  	Charset utf8 = Charset.forName("UTF-8");
  	
  	String inputStr = "A-This is\nB-the file\nC-content\n";
  	
		Files.write(inputStr, iFile, utf8);
		
		ToolRunner.run(new SortJob(), new String[]{INPUT, OUTPUT});

		String outputStr = Files.toString(new File(OUTPUT, "part-r-00000"), utf8);
		
		assertEquals(inputStr, outputStr);		
		
  	trash(INPUT, OUTPUT);
  }
}
