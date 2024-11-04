import asyncio
import json
import httpx
from supabase import create_client, Client

# Bitquery API details
url = "https://streaming.bitquery.io/eap"
payload = json.dumps({
   "query": "query TopTraders($token: String, $base: String) {\n  Solana(dataset: combined) {\n    DEXTradeByTokens(\n      orderBy: {descendingByField: \"volumeUsd\"}\n      limit: {count: 100}\n      where: {Trade: {Currency: {MintAddress: {is: $token}}, Side: {Amount: {gt: \"0\"}, Currency: {MintAddress: {is: $base}}}}, Transaction: {Result: {Success: true}}}\n    ) {\n      Trade {\n        Account {\n          Owner\n        }\n        Dex {\n          ProgramAddress\n          ProtocolFamily\n          ProtocolName\n        }\n      }\n      volume: sum(of: Trade_Amount)\n      volumeUsd: sum(of: Trade_Side_AmountInUSD)\n    }\n  }\n}\n",
   "variables": "{\n  \"token\": \"59VxMU35CaHHBTndQQWDkChprM5FMw7YQi5aPE5rfSHN\",\n  \"base\": \"So11111111111111111111111111111111111111112\"\n}"
})
headers = {
    'Content-Type': 'application/json',
    'Authorization': 'Bearer ory_at_BavWahPclrIHhewxnMRYhAcLIxxYePdzwd_'
}

# Supabase details
supabase_url = "https://xhjhkdjkjkjwqc.supabase.co"
supabase_key = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9..o2zsew68h1GdLalXdqUB_PxR3QVwu8m5vLxK3etpSJ4"
supabase: Client = create_client(supabase_url, supabase_key)

async def fetch_bitquery_data():
    async with httpx.AsyncClient() as client:
        response = await client.post(url, headers=headers, data=payload)
        print(response)
        response.raise_for_status()  # Raise an exception if the request failed
        data = response.json()
        # print(data)
        return data['data']['Solana']['DEXTradeByTokens']

async def insert_to_supabase(trades):
    print("insert_to_supabase")
    for trade in trades:
        record = {
            "owner": trade['Trade']['Account']['Owner'],
            "program_address": trade['Trade']['Dex']['ProgramAddress'],
            "protocol_family": trade['Trade']['Dex']['ProtocolFamily'],
            "protocol_name": trade['Trade']['Dex']['ProtocolName'],
            "volume": trade['volume'],
            "volume_usd": trade['volumeUsd']
        }
        print("record")
        # Insert each record into Supabase
        result = supabase.table("trades").insert(record).execute()
        print("Inserted:", result)

async def main():
    # Fetch data from Bitquery
    try:
        trades = await fetch_bitquery_data()
        # Insert data into Supabase
        await insert_to_supabase(trades)
    except httpx.HTTPStatusError as e:
        print(f"HTTP error occurred: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")

# Run the main async function
asyncio.run(main())
