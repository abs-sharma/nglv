README.md:
	echo "GuessingGame" > README.md
	date >> README.md
	echo >> README.md
	echo "Number of guesses " >> README.md
	wc -l < guessinggame.sh >> README.md
.PHONY: clean
clean:
	rm README.md
