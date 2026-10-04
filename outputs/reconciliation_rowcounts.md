## Row-count reconciliation

* arrivals where lines != header + quarantined + collapsed dupes + accepted: **0** (arrivals rejected by the quality gate are excluded: they merge nothing by design)
* (area, file_date) where silver rows != accepted rows of the owning arrival: **0**
* local days where silver rows != usage_cell_day rows != daily mart rows: **0**

### Per arrival (source -> accepted)
| arrival_id | area | file_date | status | origin | file_lines | header_lines | rows_quarantined | exact_dupes_collapsed | accepted | unexplained |
|---|---|---|---|---|---|---|---|---|---|---|
| 3 | MI | 2013-11-01 | PROCESSED | dataverse | 4842625 | 0 | 0 | 0 | 4842625 | 0 |
| 4 | TN | 2013-11-01 | PROCESSED | dataverse | 2459324 | 0 | 0 | 0 | 2459324 | 0 |
| 5 | MI | 2013-11-02 | PROCESSED | dataverse | 4745086 | 0 | 0 | 0 | 4745086 | 0 |
| 6 | TN | 2013-11-02 | PROCESSED | dataverse | 2408853 | 0 | 0 | 0 | 2408853 | 0 |
| 7 | MI | 2013-11-03 | PROCESSED | dataverse | 4722579 | 0 | 0 | 0 | 4722579 | 0 |
| 8 | TN | 2013-11-03 | PROCESSED | scenario:dirty_rows | 2258580 | 0 | 2263 | 20 | 2256297 | 0 |
| 9 | MI | 2013-11-04 | FAILED_QUALITY | scenario:quality_gate | 5639513 | 0 | 112794 | 20 | 0 |  |
| 12 | MI | 2013-11-04 | PROCESSED | dataverse:resend | 5639488 | 0 | 0 | 0 | 5639488 | 0 |
| 10 | TN | 2013-11-04 | PROCESSED | dataverse | 2347088 | 0 | 0 | 0 | 2347088 | 0 |
| 11 | MI | 2013-11-05 | PROCESSED | dataverse | 5874140 | 0 | 0 | 0 | 5874140 | 0 |
| 27 | TN | 2013-11-05 | PROCESSED | dataverse:late | 2409241 | 0 | 0 | 0 | 2409241 | 0 |
| 13 | MI | 2013-11-06 | PROCESSED | dataverse | 5912097 | 0 | 0 | 0 | 5912097 | 0 |
| 14 | TN | 2013-11-06 | PROCESSED | dataverse | 2402962 | 0 | 0 | 0 | 2402962 | 0 |
| 15 | MI | 2013-11-07 | PROCESSED | dataverse | 5886883 | 0 | 0 | 0 | 5886883 | 0 |
| 30 | MI | 2013-11-07 | PROCESSED | scenario:correction | 5886883 | 0 | 0 | 0 | 5886883 | 0 |
| 16 | TN | 2013-11-07 | PROCESSED | dataverse | 2407915 | 0 | 0 | 0 | 2407915 | 0 |
| 17 | MI | 2013-11-08 | PROCESSED | dataverse | 5918856 | 0 | 0 | 0 | 5918856 | 0 |
| 18 | TN | 2013-11-08 | PROCESSED | dataverse | 2395306 | 0 | 0 | 0 | 2395306 | 0 |
| 19 | MI | 2013-11-09 | PROCESSED | dataverse | 5052416 | 0 | 0 | 0 | 5052416 | 0 |
| 20 | TN | 2013-11-09 | PROCESSED | dataverse | 2241729 | 0 | 0 | 0 | 2241729 | 0 |
| 21 | MI | 2013-11-10 | PROCESSED | dataverse | 4731582 | 0 | 0 | 0 | 4731582 | 0 |
| 22 | TN | 2013-11-10 | PROCESSED | dataverse | 2155482 | 0 | 0 | 0 | 2155482 | 0 |
| 23 | MI | 2013-11-11 | PROCESSED | dataverse | 5478993 | 0 | 0 | 0 | 5478993 | 0 |
| 24 | TN | 2013-11-11 | PROCESSED | dataverse | 2328687 | 0 | 0 | 0 | 2328687 | 0 |
| 25 | MI | 2013-11-12 | PROCESSED | dataverse | 5597591 | 0 | 0 | 0 | 5597591 | 0 |
| 26 | TN | 2013-11-12 | PROCESSED | dataverse | 2372853 | 0 | 0 | 0 | 2372853 | 0 |
| 28 | MI | 2013-11-13 | PROCESSED | dataverse | 5689517 | 0 | 0 | 0 | 5689517 | 0 |
| 29 | TN | 2013-11-13 | PROCESSED | dataverse | 2353796 | 0 | 0 | 0 | 2353796 | 0 |
| 31 | MI | 2013-11-14 | PROCESSED | dataverse | 5703373 | 0 | 0 | 0 | 5703373 | 0 |
| 32 | TN | 2013-11-14 | PROCESSED | dataverse | 2395143 | 0 | 0 | 0 | 2395143 | 0 |
| 33 | MI | 2013-11-15 | PROCESSED | dataverse | 5708802 | 0 | 0 | 0 | 5708802 | 0 |
| 34 | TN | 2013-11-15 | PROCESSED | dataverse | 2395221 | 0 | 0 | 0 | 2395221 | 0 |
| 35 | MI | 2013-11-16 | PROCESSED | dataverse | 4847779 | 0 | 0 | 0 | 4847779 | 0 |
| 36 | TN | 2013-11-16 | PROCESSED | dataverse | 2239740 | 0 | 0 | 0 | 2239740 | 0 |
| 37 | MI | 2013-11-17 | PROCESSED | dataverse | 4611954 | 0 | 0 | 0 | 4611954 | 0 |
| 38 | TN | 2013-11-17 | PROCESSED | dataverse | 2116873 | 0 | 0 | 0 | 2116873 | 0 |
| 40 | MI | 2013-11-18 | PROCESSED | dataverse | 5417835 | 0 | 0 | 0 | 5417835 | 0 |
| 41 | TN | 2013-11-18 | PROCESSED | dataverse | 2281880 | 0 | 0 | 0 | 2281880 | 0 |
| 42 | MI | 2013-11-19 | PROCESSED | scenario:additive_column | 5577669 | 1 | 0 | 0 | 5577668 | 0 |
| 43 | TN | 2013-11-19 | PROCESSED | dataverse | 2352128 | 0 | 0 | 0 | 2352128 | 0 |
| 45 | MI | 2013-11-20 | PROCESSED | dataverse | 5643712 | 0 | 0 | 0 | 5643712 | 0 |
| 46 | TN | 2013-11-20 | PROCESSED | dataverse | 2400137 | 0 | 0 | 0 | 2400137 | 0 |
| 47 | MI | 2013-11-21 | PROCESSED | scenario:renamed_column | 5616012 | 1 | 0 | 0 | 5616011 | 0 |
| 48 | TN | 2013-11-21 | PROCESSED | dataverse | 2446418 | 0 | 0 | 0 | 2446418 | 0 |
| 49 | MI | 2013-11-22 | PROCESSED | dataverse | 5634579 | 0 | 0 | 0 | 5634579 | 0 |
| 50 | TN | 2013-11-22 | PROCESSED | dataverse | 2425626 | 0 | 0 | 0 | 2425626 | 0 |
| 51 | MI | 2013-11-23 | PROCESSED | dataverse | 4779119 | 0 | 0 | 0 | 4779119 | 0 |
| 52 | TN | 2013-11-23 | PROCESSED | dataverse | 2199957 | 0 | 0 | 0 | 2199957 | 0 |
| 53 | MI | 2013-11-24 | PROCESSED | dataverse | 4548405 | 0 | 0 | 0 | 4548405 | 0 |
| 54 | TN | 2013-11-24 | PROCESSED | dataverse | 2093272 | 0 | 0 | 0 | 2093272 | 0 |
| 55 | MI | 2013-11-25 | PROCESSED | dataverse | 5365586 | 0 | 0 | 0 | 5365586 | 0 |
| 56 | TN | 2013-11-25 | PROCESSED | dataverse | 2278609 | 0 | 0 | 0 | 2278609 | 0 |
| 57 | MI | 2013-11-26 | PROCESSED | dataverse | 5491322 | 0 | 0 | 0 | 5491322 | 0 |
| 58 | TN | 2013-11-26 | PROCESSED | dataverse | 2353253 | 0 | 0 | 0 | 2353253 | 0 |
| 59 | MI | 2013-11-27 | PROCESSED | dataverse | 5448926 | 0 | 0 | 0 | 5448926 | 0 |
| 60 | TN | 2013-11-27 | PROCESSED | dataverse | 2365414 | 0 | 0 | 0 | 2365414 | 0 |
| 61 | MI | 2013-11-28 | PROCESSED | dataverse | 5472300 | 0 | 0 | 0 | 5472300 | 0 |
| 62 | TN | 2013-11-28 | PROCESSED | dataverse | 2361764 | 0 | 0 | 0 | 2361764 | 0 |

### Per day (silver -> silver.usage_cell_day -> gold.daily_cell_usage)
| local_date | silver_rows | usage_rows | daily_mart_rows | ok |
|---|---|---|---|---|
| 2013-11-01 | 7301949 | 7301949 | 7301949 | True |
| 2013-11-02 | 7153939 | 7153939 | 7153939 | True |
| 2013-11-03 | 6978876 | 6978876 | 6978876 | True |
| 2013-11-04 | 7986576 | 7986576 | 7986576 | True |
| 2013-11-05 | 8283381 | 8283381 | 8283381 | True |
| 2013-11-06 | 8315059 | 8315059 | 8315059 | True |
| 2013-11-07 | 8294798 | 8294798 | 8294798 | True |
| 2013-11-08 | 8314162 | 8314162 | 8314162 | True |
| 2013-11-09 | 7294145 | 7294145 | 7294145 | True |
| 2013-11-10 | 6887064 | 6887064 | 6887064 | True |
| 2013-11-11 | 7807680 | 7807680 | 7807680 | True |
| 2013-11-12 | 7970444 | 7970444 | 7970444 | True |
| 2013-11-13 | 8043313 | 8043313 | 8043313 | True |
| 2013-11-14 | 8098516 | 8098516 | 8098516 | True |
| 2013-11-15 | 8104023 | 8104023 | 8104023 | True |
| 2013-11-16 | 7087519 | 7087519 | 7087519 | True |
| 2013-11-17 | 6728827 | 6728827 | 6728827 | True |
| 2013-11-18 | 7699715 | 7699715 | 7699715 | True |
| 2013-11-19 | 7929796 | 7929796 | 7929796 | True |
| 2013-11-20 | 8043849 | 8043849 | 8043849 | True |
| 2013-11-21 | 8062429 | 8062429 | 8062429 | True |
| 2013-11-22 | 8060205 | 8060205 | 8060205 | True |
| 2013-11-23 | 6979076 | 6979076 | 6979076 | True |
| 2013-11-24 | 6641677 | 6641677 | 6641677 | True |
| 2013-11-25 | 7644195 | 7644195 | 7644195 | True |
| 2013-11-26 | 7844575 | 7844575 | 7844575 | True |
| 2013-11-27 | 7814340 | 7814340 | 7814340 | True |
| 2013-11-28 | 7834064 | 7834064 | 7834064 | True |
