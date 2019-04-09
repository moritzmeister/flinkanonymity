# K-Anonymity and L-diversity in Apache Flink

This is a research project developed by [Moritz Meister](https://github.com/moritzmeister/) and [Philip Claesson](https://github.com/philipclaesson) at Politecnico di Milano.

## About
The aim of the research project is to investigate the potential in using Apache Flink's strengths of parallelizing data streams, in order to anonymize streamed data according to the [K-Anonymity](https://en.wikipedia.org/wiki/K-anonymity) and [L-Diversity](https://en.wikipedia.org/wiki/L-diversity) models.  

## Report
Find the working document of the final report [here](https://www.sharelatex.com/read/yddnznfsmsks). 

## Approach
The main novel approach of this project is to use Apache Flink's functionality to key the incoming tuples by their Quasi Identifier. By doing so, all incoming tuples with the same Quasi Identifier end up in the same process. This approach can be advantageous when: 
- minimizing data entropy loss in the anonymization step
- minimizing "late tuples" (rare tuples that are released with large delay due k tuples with same Quasi Identifier not appearing) 
