require 'kimurai'
require 'active_record'
require 'csv'

ActiveRecord::Base.establish_connection(
  adapter:  'postgresql',
  host:     'localhost',
  database: 'delco_property_reassessment_production',
  username: 'seano',
  password: ''
)

class PropertyReassessment < ActiveRecord::Base
  self.primary_key = 'parid'
end

class PropertySearchSpider < Kimurai::Base
  @name = "property_search_spider"
  @engine = :selenium_chrome
  @start_urls = ["http://delcorealestate.co.delaware.pa.us/pt/Search/Disclaimer.aspx?FromUrl=../search/commonsearch.aspx?mode=parid"]

  def parse(response, url:, data: {})
    browser.click_button("Agree")
    scrape_records
  end

  def scrape_records
    PARCEL_IDS.each do |parId|
      property = {}
      browser.visit('http://delcorealestate.co.delaware.pa.us/pt/search/commonsearch.aspx?mode=parid')
      browser.fill_in('inpParid', :with => parId)
      browser.click_button("Search")
      response = browser.current_response
      property[:parid] = response.xpath('//tr[@id="datalet_header_row"]/.//td[@class="DataletHeaderTop"][1]').inner_text.split(':')[1].strip rescue ''
      property[:site_location] = response.xpath('//table[@id="Parcel"]/tbody/tr[1]/td[2]')[0].inner_text.squeeze(" ").strip rescue ''
      property[:map_number] = response.xpath('//table[@id="Parcel"]/tbody/tr[5]/td[2]')[0].inner_text.squeeze(" ").strip rescue ''
      property[:municipality] = response.xpath('//table[@id="Parcel"]/tbody/tr[6]/td[2]')[0].inner_text.squeeze(" ").strip rescue ''
      property[:school_district] = response.xpath('//table[@id="Parcel"]/tbody/tr[7]/td[2]')[0].inner_text.squeeze(" ").strip rescue ''
      property[:property_type] = response.xpath('//table[@id="Parcel"]/tbody/tr[8]/td[2]')[0].inner_text.squeeze(" ").strip rescue ''
      property[:owner1] = response.xpath('//table[@id="Owner"]/tbody/tr[1]/td[2]')[0].inner_text.squeeze(" ").strip rescue ''
      property[:owner2] = response.xpath('//table[@id="Owner"]/tbody/tr[2]/td[2]')[0].inner_text.squeeze(" ").strip rescue ''
      property[:last_sale_date] = response.xpath('//table[@id="Owner History"]/tbody/tr[2]/td[4]').inner_text
      property[:last_sale_price] = response.xpath('//table[@id="Owner History"]/tbody/tr[2]/td[5]').inner_text
      property[:type_of_assessment] = response.xpath('//table[@id="Original Current Year Assessment"]/tbody/tr[2]/td[1]').inner_text
      property[:assessment_value] = response.xpath('//table[@id="Original Current Year Assessment"]/tbody/tr[2]/td[2]').inner_text
      property[:assessment_date] = response.xpath('//table[@id="Original Current Year Assessment"]/tbody/tr[2]/td[3]').inner_text
      property[:assessment_reason_for_change] = response.xpath('//table[@id="Original Current Year Assessment"]/tbody/tr[2]/td[4]').inner_text
      property[:assessment_comment] = response.xpath('//table[@id="Original Current Year Assessment"]/tbody/tr[2]/td[5]').inner_text
      CSV.open(DATA_FILE, "a+") do |csv|
        now = Time.now.utc
        csv << [ property[:parid], property[:site_location], property[:map_number], 
                property[:municipality], property[:school_district], property[:property_type], 
                property[:owner1], property[:owner2],
                property[:last_sale_date], property[:last_sale_price], property[:type_of_assessment], 
                property[:assessment_value], property[:assessment_date],property[:assessment_reason_for_change], 
                property[:assessment_comment], now, now ]
      end
    end
  end
end

DATA_FILE = 'data/properties.csv'
parids = []
if !File.exist?(DATA_FILE)
  CSV.open(DATA_FILE, "w") do |csv|
    csv << %w(parid site_location map_number municipality school_district property_type owner1 owner2 last_sale_date
              last_sale_price type_of_assessment assessment_value assessment_date assessment_reason_for_change
              assessment_comment created_at updated_at)
  end
else
  CSV.foreach(DATA_FILE) do |row| 
    parids << row[0] if row[0] =~ /\d+/
  end
end

# # Query database and get parcel ids for municipality that are not already in file
properties = PropertyReassessment.where.not(parid: parids)

if !properties.empty?
  PARCEL_IDS = properties.map{|property| property.parid }

  p "Scraping #{PARCEL_IDS.count} properties..."
  start_time = Time.now

  PropertySearchSpider.crawl!

  duration = Time.now - start_time
  p "Done: #{duration} seconds"
else
  p "No properties to find."
end