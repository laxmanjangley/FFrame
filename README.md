FFrame
======
expression-transformer in spark

----------------
This transformer is designed with the goal of making feature engineering simpler. It facilitates easy feature specification and improved code reusablity


Current Status
--------------
Currently the transformer works with prefix expressions:
- N-ary operands
- Nesting is allowed
- Works with any data type
- List of expressions to be worked on can be specified using a file


Evaluation function can be user specified:
- Can allow for more diverse expressions
- Type inferencing can also be done at the user level to make the transformer more airtight.


Api
---------
````scala

val lines = Source.fromFile(path_to_your_input_file).getLines.toSeq.map(l => (l.split('=')(0).trim, l.split('=')(1).trim))

type Environment = Map[String, Object]
var env: Environment = Map()

val ff = new ExprEval()
  .setFunction(parse(env) ) //the parsing function (type is (Array[(Object, Object)]) =>  (Map[String, Object]) => Object)
  .setInputCols(seq_of_your_input_cols)
  .setNumFeatures(#cols_in_your_dataframe)
  .setTt(tokenize) //tokenizer for your parser (type is String => Array[(Object, Object)])
  .setoutputTuples(lines) // seq containing (output_column, expression) tuples

ff.transform(your_dataframe)
````



