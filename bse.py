import requests
import pandas as pd
from datetime import datetime
import time
import io

def scrap_data(symbol, start_date, end_date):
    # Convert date strings to Unix timestamps
    period1 = int(time.mktime(datetime.strptime(start_date, "%Y-%m-%d").timetuple()))
    period2 = int(time.mktime(datetime.strptime(end_date, "%Y-%m-%d").timetuple()))

    # Construct the URL
    url = f"https://query1.finance.yahoo.com/v7/finance/download/{symbol}?period1={period1}&period2={period2}&interval=1d&events=history&includeAdjustedClose=true"

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }

    # Fetch the data
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        # Read the CSV data directly from the response content
        data = pd.read_csv(io.StringIO(response.text))
        data['Symbol'] = symbol
        # Process the data
        arr_data = []
        for _, row in data.iterrows():
            date = row['Date']
            open_price = row['Open']
            high = row['High']
            low = row['Low']
            close = row['Close']
            volume = row['Volume']
            symbol = row['Symbol']
            arr_data.append({
                'Date': date,
                'Open': open_price,
                'High': high,
                'Low': low,
                'Close': close,
                'Volume': volume,
                'Symbol': symbol
            })
        
        return pd.DataFrame(arr_data)  # Return DataFrame directly
    else:
        print(f"Failed to fetch data for {symbol} with status code: {response.status_code}")
        return None

def save_to_csv(data, cs):
    # Save the DataFrame to a CSV file
    data.to_csv(cs, index=False)
    print(f"Data saved to {cs}")

def process_stocks(symbols, start_date, end_date, output_filename):
    all_data = pd.DataFrame()
    
    for symbol in symbols:
        print(f"Fetching data for {symbol}...")
        data = scrap_data(symbol, start_date, end_date)
        if data is not None:
            all_data = pd.concat([all_data, data], ignore_index=True)
        else:
            print(f"Failed to fetch data for {symbol}")
    
    if not all_data.empty:
        save_to_csv(all_data, output_filename)
    else:
        print("No data fetched for any symbol.")

# Example usage
symbols =['VENTURA.BO','JAGJANANI.BO','MANBRO.BO','VARYAA.BO','MONOT.BO','SREEJAYA.BO','BHUDEVI.BO','MEDINOV.BO','KOTIC.BO','SPICEISLIN.BO'
,'AANANDALAK.BO','HINDMOTORS.BO','KSOLVES.BO','NESTLEIND.BO','GUJTLRM.BO'
,'SBECSYS.BO'
,'GRETEX.BO'
,'AKIKO.BO'
,'VINTRON.BO'
,'SWADPOL.BO'
,'TUTIALKA.BO'
,'ASSOCIATED.BO'
,'DFPL.BO'
,'ATHENAGLO.BO'
,'SAHANA.BO'
,'NPST.BO'
,'SLONE.BO'
,'MAHAPEXLTD.BO'
,'AMBIT.BO'
,'FRANKLININD.BO'
,'DIGIKORE.BO'
,'VERITAAS.BO'
,'INFRONICS.BO'
,'DENTALKART.BO'
,'PRESSURS.BO'
,'TRIVENIGQ.BO'
,'SPECFOOD.BO'
,'KPGEL.BO'
,'TIPSINDLTD.BO'
,'WAAREERTL.BO'
,'SONAMAC.BO'
,'JFL.BO'
,'PGHH.BO'
,'RISHDIGA.BO'
,'REMLIFE.BO'
,'MEAPL.BO'
,'BRIDGESE.BO'
,'TECHNVISN.BO'
,'EXHICON.BO'
,'COLPAL.BO'
,'TECHKGREEN.BO'
,'VUENOW.BO'
,'RAJKSYN.BO'
,'GAYATRI.BO'
,'INFOLLION.BO'
,'OLATECH.BO'
,'MISHTANN.BO'
,'ENFUSE.BO'
,'WOMANCART.BO'
,'ONEGLOBAL.BO'
,'CELLECOR.BO'
,'RULKA.BO'
,'KRISHCA.BO'
,'TAC.BO'
,'AMIC.BO','QUESTLAB.BO'
,'ORIANA.BO'
,'ENSER.BO'
,'21STCENMGM.BO'
,'LLOYDSME.BO'
,'CRSTCHM.BO'
,'HIGHSTREE.BO'
,'ESABINDIA.BO','QUESTLAB.BO'
,'ORIANA.BO'
,'ENSER.BO'
,'21STCENMGM.BO'
,'LLOYDSME.BO'
,'CRSTCHM.BO'
,'HIGHSTREE.BO'
,'ESABINDIA.BO'
,'KALYANI.BO'
,'SUPREMEPWR.BO'
,'SHILCTECH.BO'
,'JSLL.BO'
,'CNCRD.BO'
,'PHCAP.BO'
,'LICI.BO'
,'PRIZOR.BO'
,'NEPHROCARE.BO'
,'SANOFI.BO'
,'ACCELERATE.BO'
,'ROCKINGDCE.BO'
,'SAIFL.BO'
,'IBINFO.BO'
,'VIPULLTD.BO'
,'NINSYS.BO'
,'YURANUS.BO'
,'TRL.BO'
,'PHANTOMFX.BO'
,'KESAR.BO'
,'TROM.BO','NEXUSSURGL.bo'
,'SDL.bo'
,'JYOTIRES.BO'
,'GEMENVIRO.BO'
,'SEL.BO'
,'COCHMAL.BO'
,'WINSOL.BO'
,'BALGOPAL.BO'
,'TCS.BO'
,'INA.BO'
,'COALINDIA.BO'
,'ZTECH.BO'
,'BNRUDY.BO'
,'PROMACT.BO'
,'DHRUVCA.BO'
,'KIDUJA.BO'
,'EVERFIN.BO'
,'TRUST.BO'
,'JAIBALAJI.BO'
,'FAALCON.BO'
,'EFACTOR.BO'
,'WANBURY.BO'
,'SUMUKA.BO','ACCENTMIC.BO','KEYCORP.BO','ACCELYA.BO'
,'KODYTECH.BO'
,'WEALTH.BO'
,'CASTROLIND.BO'
,'VINSYS.BO'
,'TOTEM.BO'
,'ASPIRE.BO'
,'SANJIVIN.BO'
,'ROBU.BO'
,'SBGLP.BO'
,'BRISK.BO'
,'ESCONET.BO'
,'GLOBAL.BO'
,'SHINEFASH.BO'
,'BAWEJA.BO'
,'IRCTC.BO'
,'PRIMIND.BO'
,'NETLINK.BO'
,'ALSL.BO'
,'KOTYARK.BO'
,'VISCO.BO'
,'AIIL.BO'
,'SIGMA.BO'
,'TUNWAL.BO'
,'PULZ.BO'
,'INM.BO'
,'FTL.BO'
,'DEEPAKCHEM.BO'
,'GILLETTE.BO'
,'WTICAB.BO'
,'SWARAJENG.BO'
,'KRONOX.BO'
,'MONARCH.BO'
,'DRONE.BO'
,'INGERRAND.BO'
,'GLAXO.BO'
,'OWAIS.BO'
,'NIKSTECH.BO'
,'SHELTER.BO'
,'APS.BO'
,'K2INFRA.BO'
,'DCM.BO'
,'ANANDRATHI.BO'
,'GOYALSALT.BO'
,'GCSL.BO'
,'COMCL.BO'
,'IEX.BO'
,'DELAPLEX.BO'
,'NEWJAISA.BO'
,'SATTRIX.BO'
,'BRITANNIA.BO'
,'HOACFOODS.BO','PCCL.BO'
,'ADVANIHOTR.BO'
,'SOUTHMG.BO'
,'CAMS.BO'
,'HBSL.BO'
,'SYSTMTXC.BO'
,'GBFL.BO'
,'MSUMI.BO'
,'TTML.BO'
,'PRATHAM.BO'
,'AVANTEL.BO'
,'SAI.BO'
,'NAMAN.BO'
,'OLIL.BO'
,'CGPOWER.BO'
,'SODFC.BO'
,'KESARENT.BO'
,'ZENTEC.BO'
,'TRADEWELL.BO'
,'JNKINDIA.BO'
,'HINDZINC.BO'
,'ESCORP.BO'
,'ASAL.BO'
,'YASHOPTICS.BO'
,'ABBOTINDIA.BO'
,'DREAMFOLKS.BO'
,'CLARA.BO'
,'ALUWIND.BO'
,'EFFWA.BO'
,'JAGSONFI.BO'
,'CYBERMEDIA.BO'
,'PGHL.BO'
,'VISHNUINFR.BO'
,'ADCINDIA.BO'
,'GANESHHOUC.BO'
,'PRITHVIEXCH.BO'
,'PAGEIND.BO'
,'GUJTHEM.BO'
,'PANORAMA.BO'
,'HSIL.BO'
,'HAWKINCOOK.BO'
,'EVANS.BO'
,'RSSOFTWARE.BO'
,'ZEAL.BO'
,'SHUKRAPHAR.BO'
,'DSSL.BO'
,'MAZDOCK.BO'
,'MESON.BO'
,'APARINDS.BO'
,'TAPARIA.BO'
,'GARGI.BO'
,'PRUDENT.BO','EFORCE.BO'
,'MARICO.BO'
,'INOXINDIA.BO'
,'DHOOTIN.BO'
,'TBOTEK.BO'
,'GCONNECT.BO'
,'UNIABEXAL.BO'
,'SHARDAMOTR.BO'
,'TATAELXSI.BO'
,'BEACON.BO'
,'MACOBSTECH.BO'
,'PARAGON.BO'
,'ACE.BO'
,'AKZOINDIA.BO'
,'ALPHAIND.BO'
,'SHREE.BO'
,'AEIM.BO'
,'CGRAPHICS.BO'
,'DDEVPLASTIK.BO'
,'MANAV.BO'
,'VADILENT.BO'] # List of stock symbols
start_date = '2015-01-01'
end_date = '2024-06-30'
output_filename = 'multiple_companies_data.csv'
process_stocks(symbols, start_date, end_date, output_filename)