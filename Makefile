# This is the Makefile helping you submit the labs.
# Just create 6.824/api.key with your API key in it,
# and submit your lab with the following command:
#     $ make [lab1|lab2a|lab2b|lab2c|lab2d|lab3a|lab3b|lab4a|lab4b]

LABS=" lab1 lab2a lab2b lab2c lab2d lab3a lab3b lab4a lab4b "

%: check-%
	@echo "Preparing $@-handin.tar.gz"
	@if echo $(LABS) | grep -q " $@ " ; then \
		echo "Tarring up your submission..." ; \
		COPYFILE_DISABLE=1 tar cvzf $@-handin.tar.gz \
			"--exclude=src/main/pg-*.txt" \
			"--exclude=src/main/diskvd" \
			"--exclude=src/mapreduce/824-mrinput-*.txt" \
			"--exclude=src/main/mr-*" \
			"--exclude=mrtmp.*" \
			"--exclude=src/main/diff.out" \
			"--exclude=src/main/mrmaster" \
			"--exclude=src/main/mrsequential" \
			"--exclude=src/main/mrworker" \
			"--exclude=*.so" \
			Makefile src; \
		if ! test -e api.key ; then \
			echo "Missing $(PWD)/api.key. Please create the file with your key in it or submit the $@-handin.tar.gz via the web interface."; \
		else \
			echo "Are you sure you want to submit $@? Enter 'yes' to continue:"; \
			read line; \
			if test "$$line" != "yes" ; then echo "Giving up submission"; exit; fi; \
			if test `stat -c "%s" "$@-handin.tar.gz" 2>/dev/null || stat -f "%z" "$@-handin.tar.gz"` -ge 20971520 ; then echo "File exceeds 20MB."; exit; fi; \
			cat api.key | tr -d '\n' > .api.key.trimmed ; \
			curl --silent --fail --show-error -F file=@$@-handin.tar.gz -F "key=<.api.key.trimmed" \
			https://6824.scripts.mit.edu/2021/handin.py/upload > /dev/null || { \
				echo ; \
				echo "Submit seems to have failed."; \
				echo "Please upload the tarball manually on the submission website."; } \
		fi; \
	else \
		echo "Bad target $@. Usage: make [$(LABS)]"; \
	fi

.PHONY: check-%
check-%:
	@echo "Checking that your submission builds correctly..."
	@./.check-build git://g.csail.mit.edu/6.824-golabs-2021 $(patsubst check-%,%,$@)
