# This is the Makefile helping you submit the labs.  
# Just create 6.824/api.key with your API key in it, 
# and submit your lab with the following command: 
#     $ make [lab1|lab2a|lab2b|lab2c|lab3a|lab3b|lab4a|lab4b]

LABS=" lab1 lab2a lab2b lab2c lab3a lab3b lab4a lab4b "

%:
	@echo "Preparing $@-handin.tar.gz"
	@echo "Checking for committed temporary files..."
	@if git ls-files | grep -E 'mrtmp|mrinput' > /dev/null; then \
		echo "" ; \
		echo "OBS! You have committed some large temporary files:" ; \
		echo "" ; \
		git ls-files | grep -E 'mrtmp|mrinput' | sed 's/^/\t/' ; \
		echo "" ; \
		echo "Follow the instructions at http://stackoverflow.com/a/308684/472927" ; \
		echo "to remove them, and then run make again." ; \
		echo "" ; \
		exit 1 ; \
	fi
	@if echo $(LABS) | grep -q " $@ " ; then \
		echo "Tarring up your submission..." ; \
		tar cvzf $@-handin.tar.gz \
			"--exclude=src/main/pg-*.txt" \
			"--exclude=src/main/diskvd" \
			"--exclude=src/mapreduce/824-mrinput-*.txt" \
			"--exclude=mrtmp.*" \
			"--exclude=src/main/diff.out" \
			Makefile src; \
		if ! test -e api.key ; then \
			echo "Missing $(PWD)/api.key. Please create the file with your key in it or submit the $@-handin.tar.gz via the web interface."; \
		else \
			echo "Are you sure you want to submit $@? Enter 'yes' to continue:"; \
			read line; \
			if test "$$line" != "yes" ; then echo "Giving up submission"; exit; fi; \
			if test `stat -c "%s" "$@-handin.tar.gz" 2>/dev/null || stat -f "%z" "$@-handin.tar.gz"` -ge 20971520 ; then echo "File exceeds 20MB."; exit; fi; \
			mv api.key api.key.fix ; \
			cat api.key.fix | tr -d '\n' > api.key ; \
			rm api.key.fix ; \
			curl -F file=@$@-handin.tar.gz -F "key=<api.key" \
			https://6824.scripts.mit.edu/2017/handin.py/upload > /dev/null || { \
				echo ; \
				echo "Submit seems to have failed."; \
				echo "Please upload the tarball manually on the submission website."; } \
		fi; \
	else \
		echo "Bad target $@. Usage: make [$(LABS)]"; \
	fi
