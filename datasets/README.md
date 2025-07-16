# ContextAwareJoin -- Datasets


## Folder Structure

```bash
datasets
├── README.md                       # this file
├── download.sh                     # Run this script to get the datasets in the required format.
|  
├── autojoin/               
|  ├── datalake/               # This will be generated after `download.sh`
|  ├── gt.jsonl                # The ground truth file
|  └── create_datalake.sh      # Script to covert the original dataset into a datalake
|  
├── wikijoin
|  ├── datalake/               # This will be generated after `download.sh`
|  └── gt.jsonl.tar.gz         # The ground truth file; this will be converted to gt.jsonl after `download.sh`
|  
├── nextia/   
|  ├── testbedM/               # NextiaJD Medium dataset
|  |  ├── datalake/            # This will be generated after `download.sh`
|  |  └── gt.jsonl             # The ground truth file
|  |  
|  └── testbedS/               # NextiaJD Small dataset
|     ├── datalake/            # This will be generated after `download.sh`
|     └── gt.jsonl             # The ground truth file
|
├── opendata/              # This folder will be generated after `download.sh`
|     ├── datalake/         # Tables 
|     ├── metadata/         # Metadata 
|     └── gt.jsonl          # Ground Truth File
|  
├── valentine/              # Valentine dataset
|  
└── gosales/                 
   ├── datalake/            # Generate manually after `download.sh`
   └── gt.jsonl             # The ground truth file

```

## References



The Datasets folder contains the datasets used in the evaluations.

1. **AutoJoin**


    Paper: [Erkang Zhu, Yeye He, Surajit Chaudhuri: Auto-Join: Joining Tables by Leveraging Transformations. Proc. VLDB Endow. 10(10): 1034-1045 (2017)](http://www.vldb.org/pvldb/vol10/p1034-he.pdf)
    
    Original Data Link: https://github.com/Yeye-He/Auto-Join

2. **WikiJoin**
    
    Paper: [Kavitha Srinivas, Julian Dolby, Ibrahim Abdelaziz, Oktie Hassanzadeh, Harsha Kokel, Aamod Khatiwada, Tejaswini Pedapati, Subhajit Chaudhury, Horst Samulowitz: LakeBench: Benchmarks for Data Discovery over Data Lakes. CoRR abs/2307.04217 (2023)](https://doi.org/10.48550/arXiv.2307.04217)
    
    Original Data Link: https://zenodo.org/records/10042019

3. **NextiaJD**

    Paper: [Javier Flores, Sergi Nadal, Oscar Romero: Effective and Scalable Data Discovery with NextiaJD. EDBT 2021: 690-693](https://openproceedings.org/2021/conf/edbt/p184.pdf)

    Original Data Link: https://www.essi.upc.edu/~jflores/nextiajd.html 

4. **Valentine**  
    
    Paper: [Christos Koutras, George Siachamis, Andra Ionescu, Kyriakos Psarakis, Jerry Brons, Marios Fragkoulis, Christoph Lofi, Angela Bonifati, Asterios Katsifodimos: Valentine: Evaluating Matching Techniques for Dataset Discovery. ICDE 2021: 468-479](https://doi.org/10.1109/ICDE51399.2021.00047)
    
    Original Data Link: https://doi.org/10.5281/zenodo.5084605 

5. **GoSales**

    Original Data Link: [IBM Cognos Analytics](https://accelerator.ca.analytics.ibm.com/bi/?perspective=authoring&pathRef=.public_folders%2FIBM%2BAccelerator%2BCatalog%2FContent%2FDEP00001&id=iD268937B6FDA49679A7F69574B242692)


6. **OpenData**

    Data Link: [![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.15881731.svg)](https://doi.org/10.5281/zenodo.15881731)
