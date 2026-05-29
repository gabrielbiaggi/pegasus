import json
d=json.load(open('/tmp/bt.json'))
r=d.get('results',[])
sn=['tp30_flat_sl','tp30_pct2_wr','tp30_s25_pct2_wr','tp50_flat_sl','tp50_pct2_wr','tp50_s25_pct2_wr','tp80_flat_sl','tp80_pct2_wr','tp80_s25_pct2_wr']
short=['30fs','30p2','30s25','50fs','50p2','50s25','80fs','80p2','80s25']
print('Status:',d['status'],'Dia:',d.get('current_day'),'Dias:',len(r),'/',d.get('total_days'),'Elapsed:',int(d.get('elapsed_s',0)),'s')
print()
hdr = 'Data       '
for s in short: hdr += s.rjust(6)
print(hdr)
print('-'*len(hdr))
for x in r:
    st=x.get('strategies',{})
    line=x['date']+' '
    for s in sn:
        sr=st.get(s,{})
        b=round(sr.get('balance',0))
        wr=sr.get('signal_wr',0)
        mark = 'X' if sr.get('busted') else ('S' if sr.get('stopped_sl') else '')
        line+=('$'+str(b)+mark).rjust(6)
    print(line)
s2=d.get('summary',{}).get('strategies',{})
if s2:
    print()
    print('='*75)
    print('RANKING FINAL (18 dias, $50 inicial):')
    print('-'*75)
    ranked=sorted(s2.items(), key=lambda x: x[1].get('final_balance',0), reverse=True)
    for i,(s,ss) in enumerate(ranked):
        fb=ss.get('final_balance',0)
        tp=ss.get('total_pnl',0)
        roi=ss.get('roi_pct',0)
        bd=ss.get('busted_days',0)
        pd2=ss.get('positive_days',0)
        td=ss.get('total_days',18) if 'total_days' in ss else d.get('summary',{}).get('total_days',18)
        medal = ['1st','2nd','3rd'][i] if i<3 else str(i+1)+'th'
        status = 'BUST' if fb < 1 else ('LUCRO' if tp > 0 else 'PERDA')
        print(f'  {medal:4} {s:20} ${fb:>7.2f}  PnL:{tp:>+7.2f}  ROI:{roi:>+6.1f}%  Bust:{bd}  Dias+:{pd2}  [{status}]')
