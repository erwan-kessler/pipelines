# PIPELINES


What the program should do for input lines that do not fit the message format?

It should log them into whatever fits and discard them, it should not crash.
If it is possible we could add some logic to reparse some data but in general if bad data comes in, 
it's better to correct the source than to do so at the ingest.