for i in 0 1 2 3 4 5 6 7 8 9
do
    echo "Processing 00${i}..."
    poetry run python pubmed_data/process.py \
        --filelist ./assets/00$i/oa_comm_xml.PMC00${i}xxxxxx.baseline.2023-02-08.filelist.csv \
        --tar=./assets/00$i/oa_comm_xml.PMC00${i}xxxxxx.baseline.2023-02-08.tar.gz
done
