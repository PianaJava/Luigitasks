# Luigitasks

Run multiple luigi tasks in series and parallel.


# From Dev to Prod

- Starting a Pipeline
    In dev  -> use the local scheduler to run luigi tasks or with luigid
            Can also hard-code parameters on here
            Instead of the hard coded paramenters, to avoid mistakes, it's possible to importo using LUIGI_CONFIG_PATH (both on dev and prod) to show which config file to use

    In prod -> Run luigid, can also run on another host
            Move the Hardocoded parameters on luigi.cfg file 
            Instead of the luigi.cfg, to void mistakes, it's possible to importo using LUIGI_CONFIG_PATH (both on dev and prod) to show which config file to use

- Traking pipeline progress
    - use luigid
    - use set_progress_percentage()
    - use workers for speeding up, but do it smart