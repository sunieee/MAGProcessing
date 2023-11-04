
while pgrep -f "aria2c" > /dev/null; do
    echo "Waiting for aria2c to finish at $(date)"
    sleep 10
done

bunzip2 -k /home/datahouse/PaperAbstracts.nt.bz2