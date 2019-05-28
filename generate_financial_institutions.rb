#!/usr/bin/ruby
# frozen_string_literal: true

require 'json'
require 'mongo'

def format_branch(bank_code,current_branch)
  {
      Code:bank_code,
      BranchNumber:current_branch['code'].to_i,
      BranchNameKanji:current_branch['name'],
      BranchNameKana:current_branch['kana']
  }
end

def format_bank(current_bank)
  {
      Code: current_bank['code'].to_i,
      NameKanji: current_bank['name'],
      NameKana: current_bank['kana'],
      branches: []
  }
end

def insert_documents(financial_institutions_docs,my_financialinstitution)
  Mongo::Logger.logger.level = Logger::WARN
  client_host = ['HOST_NAME']
  client_options = {
    database: 'DB_NAME',
    user: 'USER',
    password: 'PASSWORD',
    ssl: true
  }
  # client_host = [ '127.0.0.1:27017' ]
  # client_options ={
  #   database:'Metadata'
  # }
  client = Mongo::Client.new(client_host, client_options)
  client.collections.each { |coll| puts coll.name }

  collection = client[my_financialinstitution]

  insert_result = collection.insert_many(financial_institutions_docs) 
  
  puts(insert_result.inserted_count)
  
  client.close
end

def parse_data(path)
  banks = JSON.parse(File.read("data/banks.json"))
  branches_path = "data/branches"

  financial_institutions_docs = []
  branches_docs = []

  banks.keys.each do |bank_code|
    specific_formatted_bank = format_bank(banks[bank_code])
    financial_institutions_docs.push(specific_formatted_bank);

    branches = JSON.parse(File.read("#{branches_path}/#{bank_code}.json"))
    branches.keys.each do |branch_number|
      current_branch = branches[branch_number]
      specific_formatted_branch = format_branch(bank_code,current_branch)
      branches_docs.push(specific_formatted_branch)
    end
  end

  puts(financial_institutions_docs.to_json)
  insert_documents(financial_institutions_docs,:FinancialInstitution)

  puts(branches_docs.to_json)
  insert_documents(branches_docs,:Branch)
  
end

path = ARGV[0]
parse_data(path)



