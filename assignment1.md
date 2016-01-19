**Question 1:**

**LinesNum.java:**

I'm using this job to compute total line number and number of lines that each word shows up in a line. The output will be used as side data in ParisPMI & StripesPMI. It consisits 1 MapReduce job.
Input is line of words; intermediate key-value pairs are word and its associated "apperance" number; output is pairs of word and its associated TOTAL "apperance".

**PairsPMI.java:**

I'm using this job to compute the PMI value of two words, using "Pair" method. It consists 1 MapReduce job. Input is line of words; intermediate key-value pairs - pairs of words(i.e (x,y)) and its "apperance" number; output - pairs of word and its TOTAL "apperance".

**StripesPMI.java:**

I'm using this job to compute the PMI value of two words, using "Stripe" method. It consists 1 MapReduce job. Input is line of words; intermediate key-value pairs - hash map of string(i.e x) to float(i.e y's apperace); Output is - hash map of string(i.e x) to float(i.e PMI value of x&y)



**Question 2: Ran under school linux environment**

Pairs: 48.201 seconds

Ptripes: 16.122 seconds



**Question 3: Ran under school linux environment**

pairs: 60.156 seconds

stripes: 18.146 seconds



**Question 4:**

  77198  231594 1858748



**Question 5:**

(maine, anjou)	3.6331422

(anjou, maine)	3.6331422

(milford, haven)	3.6201773

(haven, milford)	3.6201773

(cleopatra's, alexandria)	3.5387795

(alexandria, cleopatra's)	3.5387795

(rosencrantz, guildenstern)	3.5383153

(guildenstern, rosencrantz)	3.5383153

(personae, dramatis)	3.5316846

(dramatis, personae)	3.5316846

High PMI value may result from low probablity of apperace(i.e low p(x)) from both words, thus p(x)*p(y) would be very small, and it would make p(x,y)/p(x)p(y) a great value.




**Question 6:**

(tears, shed)	2.1117902

(tears, salt)	2.052812

(tears, eyes)	1.165167

(death, father's)	1.120252

(death, die)	0.7541594

(death, life)	0.7381346




