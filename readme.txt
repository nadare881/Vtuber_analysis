分析に用いたデータの一部(Vtuber共有用.csv)を手作業で修正しています
以下の手順でやると大体似たようなデータセットを作れます

1. CHJ氏のデータセットをDLし、F列までを"Vtuber共有用.csv"に変換して保存
   https://docs.google.com/spreadsheets/d/1hAO3FV32vPkAeIYlobUfvc_nG-hJsV2q0AKU8Pt0DtA/edit#gid=1468928798
2. userlocal_scraper.py でuserlocal.csvを作成
3. userlocal.csvを元にtwitter_scraper.py でtwitter.csvを作成
4. userlocal.csvを元にyoutube_scraper.py でyoutube.csvを作成
5. 上記の4つのファイルをfile_merge.pyで繋げる


