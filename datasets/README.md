# ContextAwareJoin -- Datasets

All the datasets used in the paper are publicly available, except the CIO dataset. 

## Getting Started 

To download the datasets and covert to format compatible with the code run the following script.

```bash
cd datasets
./download.sh
```


## Folder Structure

```bash
datasets
├── README.md                       # this file
├── download.sh                     # Run this script to get the datasets in the required format.
|  
|  
├── autojoin/               
|  ├── datalake/               # This will be generated after `download.sh`
|  ├── datalake/               # This will be generated after `download.sh`
|  ├── gt.jsonl                # The ground truth file
|  └── create_datalake.sh      # Script to covert the original dataset into a datalake
|  
|  
├── wikijoin
|  ├── datalake/               # This will be generated after `download.sh`
|  ├── datalake/               # This will be generated after `download.sh`
|  └── gt.jsonl.tar.gz         # The ground truth file; this will be converted to gt.jsonl after `download.sh`
|  
|  
├── nextia/   
|  ├── testbedM/               # NextiaJD Medium dataset
|  |  ├── datalake/            # This will be generated after `download.sh`
|  |  ├── datalake/            # This will be generated after `download.sh`
|  |  └── gt.jsonl             # The ground truth file
|  |  
|  |  
|  └── testbedS/               # NextiaJD Small dataset
|     ├── datalake/            # This will be generated after `download.sh`
|     ├── datalake/            # This will be generated after `download.sh`
|     └── gt.jsonl             # The ground truth file
|
├── opendata/              # This folder will be generated after `download.sh`
|  ├── datalake/         # Tables 
|  ├── metadata/         # Metadata 
|  └── gt.jsonl          # Ground Truth File
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

     [![Auto-Join](https://img.shields.io/badge/GitHub-Auto_Join-8640BF)](https://github.com/Yeye-He/Auto-Join)


    Paper: [Erkang Zhu, Yeye He, Surajit Chaudhuri: Auto-Join: Joining Tables by Leveraging Transformations. Proc. VLDB Endow. 10(10): 1034-1045 (2017)](http://www.vldb.org/pvldb/vol10/p1034-he.pdf)
    


2. **WikiJoin**
    
    [![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.10042019.svg)](https://doi.org/10.5281/zenodo.10042019)

    Paper: [Kavitha Srinivas, Julian Dolby, Ibrahim Abdelaziz, Oktie Hassanzadeh, Harsha Kokel, Aamod Khatiwada, Tejaswini Pedapati, Subhajit Chaudhury, Horst Samulowitz: LakeBench: Benchmarks for Data Discovery over Data Lakes. CoRR abs/2307.04217 (2023)](https://doi.org/10.48550/arXiv.2307.04217)
    

3. **NextiaJD**

    [![NextiaJD](https://img.shields.io/badge/URL-NextiaJD-24D6D4)]( https://www.essi.upc.edu/~jflores/nextiajd.html)
    
    Paper: [Javier Flores, Sergi Nadal, Oscar Romero: Effective and Scalable Data Discovery with NextiaJD. EDBT 2021: 690-693](https://openproceedings.org/2021/conf/edbt/p184.pdf)

   

4. **Valentine**  

    [![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.5084605.svg)](https://doi.org/10.5281/zenodo.5084605)
    
    Paper: [Christos Koutras, George Siachamis, Andra Ionescu, Kyriakos Psarakis, Jerry Brons, Marios Fragkoulis, Christoph Lofi, Angela Bonifati, Asterios Katsifodimos: Valentine: Evaluating Matching Techniques for Dataset Discovery. ICDE 2021: 468-479](https://doi.org/10.1109/ICDE51399.2021.00047)
    

5. **GOSales**


    [![GOSales](https://img.shields.io/badge/URL-IBM_Cognos_Analytics-24D6D4)](https://accelerator.ca.analytics.ibm.com/bi/?perspective=authoring&pathRef=.public_folders%2FIBM%2BAccelerator%2BCatalog%2FContent%2FDEP00001&id=iD268937B6FDA49679A7F69574B242692)



6. **OpenData**

    [![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.15881731.svg)](https://doi.org/10.5281/zenodo.15881731)

    Paper: [Harsha Kokel, Aamod Khatiwada, Tejaswini Pedapati, Haritha Ananthakrishnan, Oktie Hassanzadeh, Horst Samulowitz, and Kavitha Srinivas. TOPJoin: A Context-Aware Multi-Criteria Approach for Joinable Column Search. VLDB 2025 Workshop: Tabular Data Analysis (TaDA).](https://arxiv.org/abs/2507.11505)
    
