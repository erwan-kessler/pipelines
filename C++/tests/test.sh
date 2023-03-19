#!/usr/bin/env sh

# shellcheck disable=SC2002
cat fixture_in.txt | ./pipeline 2>err.txt 1>out.txt

echo "## DIFF OUT##"
echo ""
diff fixture_out.txt out.txt -wB
echo ""
echo "##############"

echo "## DIFF ERR##"
echo ""
diff fixture_err.txt err.txt -wB
echo ""
echo "##############"
