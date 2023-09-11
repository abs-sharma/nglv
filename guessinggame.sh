files_count(){
	local count=$(ls -1 | wc -l)
	echo "$count"
}

while true; do
	echo "Guess number of files"
	read guess
	filec=$(files_count)
	if [[ $guess -lt $filec ]]; then
		echo "Higher! Try"
	elif [[ $guess -gt $filec ]]; then 
		echo "Lower! Try"
	else
		echo "Good!"
		break
	fi
done

