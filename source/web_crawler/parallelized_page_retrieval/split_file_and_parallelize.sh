#!/usr/bin/screen -d -m -S /bin/bash
rm -f /root/url_lists/mergedfile.txt
cat /root/url_lists/logfile*.txt > /root/url_lists/mergedfile.txt
mv /root/url_lists/logfile*.txt /root/processed_url_lists
find /root/urls/ -type f -name F\* -exec rm {} \;
file_length=`sed -n '$=' /root/url_lists/mergedfile.txt`
file_splits=300
split_length=$(echo "$file_length / $file_splits" | bc)
awk -v var=$split_length 'NR%var+0==1{x="F"++i;}{print > x}' /root/url_lists/mergedfile.txt
for i in {1..301}; do
  python3.5 /root/html_retrieval_and_storage.py /root/urls/F$i &
done
