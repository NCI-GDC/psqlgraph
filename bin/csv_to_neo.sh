#!/bin/bash
while getopts “:s:d:b:” OPTION
do
    case $OPTION in
    s)
        csv_dir=$OPTARG
        ;;
    d)
        db_dir=$OPTARG
        ;;
    b)
        import_dir=$OPTARG
        ;;
    esac
done
shift $((OPTIND-1))
cmd=$1
if [ -z "$cmd" ]
then 
cat <<EOF
To setup batch importer, run
    $0 setup
To convert data from csv to neo4j db, run
    $0 convert
    arguments:     -s      csv source directory
                   -d      data directory
                   -b batch importer location 
EOF
exit 0 
elif [ "$cmd" = "setup" ];
then
    echo "$cmd"
    wget https://dl.dropboxusercontent.com/u/14493611/batch_importer_20.zip
    unzip batch_importer_20.zip -d batch_importer && rm batch*.zip
exit 0
elif [ "$cmd" = "convert" ];
then

    if  [ -z "$csv_dir" ] ||[  -z "$db_dir" ] || [ -z "$import_dir" ]
    then
        echo "please give csv, importer, db directory: 
    ./csv_to_neo.sh -s csv_dir -b import_dir -d data_dir convert"
    else
    cd $import_dir
    files=$(readlink -f $csv_dir/nodes*.csv | xargs | sed -e 's/ /,/g')
    ./import.sh $db_dir $files $csv_dir/rels.csv 

    fi
fi
