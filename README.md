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


Coding Style
------------




License
-------
This is free and unencumbered software released into the public domain.

Anyone is free to copy, modify, publish, use, compile, sell, or
distribute this software, either in source code form or as a compiled
binary, for any purpose, commercial or non-commercial, and by any
means.

In jurisdictions that recognize copyright laws, the author or authors
of this software dedicate any and all copyright interest in the
software to the public domain. We make this dedication for the benefit
of the public at large and to the detriment of our heirs and
successors. We intend this dedication to be an overt act of
relinquishment in perpetuity of all present and future rights to this
software under copyright law.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
OTHER DEALINGS IN THE SOFTWARE.


Issues
------
Typing has not been handled in a proper way.



