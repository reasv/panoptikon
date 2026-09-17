#!/bin/sh
# Stage the final pass's results on the Mac: drop root/, gzip anything big.
set -u
R=/Users/moot/projects/panoptikon-pr27
T=$R/tools/calibration-protocol
STAGE=$R/.mac/final/stage
rm -rf $STAGE
mkdir -p $STAGE/legs $STAGE/instruments $STAGE/analysis

for d in $T/results/fin-*/; do
  rid=$(basename $d)
  case "$rid" in
    fin-mixed) dst=$STAGE/mixed ;;
    fin-a1-cpu) dst=$STAGE/a1-cpu ;;
    fin-a1-auto) dst=$STAGE/a1-auto ;;
    *) dst=$STAGE/legs/$rid ;;
  esac
  mkdir -p $dst
  (cd $d && tar cf - --exclude=root --exclude=__pycache__ .) | (cd $dst && tar xf -)
done

cp $R/.mac/final/mps-selftest.json $R/.mac/final/mps-selftest-oom.json \
   $R/.mac/final/selftest.log $R/.mac/final/selftest-oom.log \
   $R/.mac/final/platform.txt $R/.mac/final/pytest.log \
   $R/.mac/final/build.log $R/.mac/final/uvsync.log \
   $R/.mac/final/setup.status $R/.mac/final/legs.status \
   $R/.mac/final/extras.status $R/.mac/final/corpora.status \
   $R/.mac/final/*.sh $R/.mac/final/*.py \
   $R/.mac/final/server-C1-a1cpu.toml $R/.mac/final/server-C1-a1auto.toml \
   $STAGE/instruments/ 2>/dev/null
cp $R/.mac/server-C1-mac.toml $STAGE/instruments/server-C1-mac.toml 2>/dev/null
cp $R/.mac/run5/server-C8-mac.toml $STAGE/instruments/server-C8-mac.toml 2>/dev/null
cp $R/.mac/run5/registry-C8/registry-C8.toml $STAGE/instruments/registry-C8.toml 2>/dev/null
for f in $R/.mac/final/S1.log $R/.mac/final/S2-*.log $R/.mac/final/S3-*.log \
         $R/.mac/final/S4*.log $R/.mac/final/S14-*.log $R/.mac/final/corpus-*.log; do
  [ -f "$f" ] && cp "$f" $STAGE/instruments/
done
cp $R/.mac/final/analyze-fin/analyze.txt $STAGE/analysis/analyze-2133563f.txt 2>/dev/null
cp $R/.mac/final/analyze-r4/analyze.txt $STAGE/analysis/analyze-0d7f5671.txt 2>/dev/null
cp $R/.mac/final/analyze-r4fix/analyze.txt $STAGE/analysis/analyze-holdfix-0d7f5671.txt 2>/dev/null
cp $R/.mac/final/*.json $STAGE/analysis/ 2>/dev/null
rm -f $STAGE/analysis/mps-selftest.json $STAGE/analysis/mps-selftest-oom.json

find $STAGE -type f -size +100k ! -name "*.gz" -exec gzip -9 {} +
du -sh $STAGE
echo "STAGED"
