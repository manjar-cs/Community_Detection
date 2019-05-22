# Community_Detection

## Construction of social ntework graph to find different communities

Each	node represents	a	user and	there	will	be	an	edge	between	two	nodes	if the	number	of	times	that	two	users	review the	same	business is	greater than or	equivalent to the	filter	threshold. For	example,	suppose	user1	reviewed [business1, business2, business3] and user2 reviewed	[business2, business3, business4, business5].	If	the	threshold	is 2,	there	will	be an edge	between	user1	and	user2.

#### Programming	Environment:

1. Spark	DataFrame	and	GraphFrames library	for	task1,	but	for	task2	i have used only Spark RDD and standard	Python or Scala libraries.
2. Python	3.6	and	Spark	2.3.2

## Dataset Used :

Yelp dataset- can be downloaded from here (https://www.yelp.com/dataset). I have used a sample of this dataset which can be downloaded from here (https://github.com/manjar-cs/Community_Detection/blob/master/ub_sample_data.csv)

## Task1:	Community	Detection	Based	on	GraphFrames

In task1, I have used Spark	GraphFrames library	to	detect	communities	in	the	network	graph.
In	the	library,	it provides the	implementation	of	the	Label	Propagation	Algorithm	(LPA) which	was	proposed	by	Raghavan,	Albert,	and	Kumara	in	 2007.	It	is	an	iterative	community	detection	solution	whereby	 information	 “flows”	 through	 the	graph	 based	 on	 underlying	edge	 structure.	In	this	task,	I	did	not implement	the	algorithm	from	scratch,	I used the	method	provided	by	the library.

## Task2:	Community	Detection	Based	on	Girvan-Newman	algorithm

### 1) Betweenness Calculation:

In	this	part,	I have calculated	the	betweenness	of	each	edge in the	original graph I constructed. Then	you	need to	save	your	result	in	a	txt file.	
The	format	of	each	line	is	(‘user_id1’,	‘user_id2’),	betweenness	value

Result	should	be	firstly sorted	by	the	betweenness	values in	the	descending	order and	then	the	first	
user_id	in	the	tuple	in	lexicographical order	(the	user_id	is type	of	string). The	two	user_ids	in	each	tuple	
should	also	in	lexicographical order.

### 2) Community Detection:

You	 are	 required	 to	 divide	 the	 graph	 into	 suitable	 communities,	 which	 reaches	 the	 global	 highest	
modularity.	

The	formula	of	modularity	is	shown	below:

![Formula](https://github.com/manjar-cs/Community_Detection/blob/master/images/Formula.png?raw=true "Title")

According	 to	 the	 Girvan-Newman	 algorithm,	 after	 removing one	 edge,	 you	 should re-compute	 the	
betweenness. The	“m”	in the	formula	represents	the	edge	number	of	the	original	graph. The	“A”	in	the	
formula	is	the	adjacent	matrix	of	the	original	graph.	(Hint:	In	each	remove	step,	“m”	and	“A”	should	not	
be	changed).

If	the	community	only	has	one	user	node,	we	still	regard	it	as	a	valid	community.

## Execution format:
#### Task1:

For	Python:

• In	PyCharm,	you	need	to	add	the	sentence	below	into	your	code
```
pip install graphframes

os.environ["PYSPARK_SUBMIT_ARGS"] = ("--packages graphframes:graphframes:0.6.0-spark2.3-s_2.11")
```
• In	the	terminal,	you	need	to	assign	the	parameter	“packages”	of	the	spark-submit:
```
--packages graphframes:graphframes:0.6.0-spark2.3-s_2.1

spark-submit --packages graphframes:graphframes:0.6.0-spark2.3-s_2.11 firstname_lastname_task1.py <filter	threshold> <input_file_path> <community_output_file_path>
```
#### Task 2:
```
spark-submit firstname_lastname_task2.py <filter threshold> <input_file_path> <betweenness_output_file_path> <community_output_file_path>
```

Input	parameters:	
1.	filter	threshold:	the	filter	threshold to	generate	edges	between	user	nodes.
2.	input	file	path:	the	path	to	the	input	file	including	path,	file	name	and	extension.
3.	betweenness	output file	path:	the	path	to	the	betweenness	output	file	including	path,	file	name and extension.
4.	community	output	file	path: the	path	to	the	community	output	file	including	path,	file	name	and	
extension.

