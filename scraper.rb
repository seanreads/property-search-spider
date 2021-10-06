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
      browser.visit("http://delcorealestate.co.delaware.pa.us/pt/Datalets/PrintDatalet.aspx?pin=#{parId}&gsp=PROFILEALL_PUB&taxyear=2021&jur=023&ownseq=0&card=1&roll=REAL&State=1&item=1&items=-1&all=all&ranks=Datalet")
      response = browser.current_response
      property[:parid] = response.xpath('//tr[@class="DataletHeaderTop"][1]/td[1]').inner_text.split(':')[1].strip rescue ''
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
      property[:grade] = response.xpath('//table[@id="Residential"]/tbody/tr[3]/td[2]')[0].inner_text.squeeze(" ").strip rescue ''
      property[:cdu] = response.xpath('//table[@id="Residential"]/tbody/tr[4]/td[2]')[0].inner_text.squeeze(" ").strip rescue ''
      property[:style] = response.xpath('//table[@id="Residential"]/tbody/tr[5]/td[2]')[0].inner_text.squeeze(" ").strip rescue ''
      property[:acres] = response.xpath('//table[@id="Residential"]/tbody/tr[6]/td[2]')[0].inner_text.squeeze(" ").strip rescue ''
      property[:year_built] = response.xpath('//table[@id="Residential"]/tbody/tr[7]/td[2]')[0].inner_text.squeeze(" ").strip rescue ''
      property[:remodeled_year] = response.xpath('//table[@id="Residential"]/tbody/tr[8]/td[2]')[0].inner_text.squeeze(" ").strip rescue ''
      property[:base_area] = response.xpath('//table[@id="Residential"]/tbody/tr[10]/td[2]')[0].inner_text.squeeze(" ").strip rescue ''
      property[:finished_basement_area] = response.xpath('//table[@id="Residential"]/tbody/tr[11]/td[2]')[0].inner_text.squeeze(" ").strip rescue ''
      property[:number_of_stories] = response.xpath('//table[@id="Residential"]/tbody/tr[12]/td[2]')[0].inner_text.squeeze(" ").strip rescue ''
      property[:exterior_wall] = response.xpath('//table[@id="Residential"]/tbody/tr[14]/td[2]')[0].inner_text.squeeze(" ").strip rescue ''
      property[:basement] = response.xpath('//table[@id="Residential"]/tbody/tr[15]/td[2]')[0].inner_text.squeeze(" ").strip rescue ''
      property[:physical_condition] = response.xpath('//table[@id="Residential"]/tbody/tr[16]/td[2]')[0].inner_text.squeeze(" ").strip rescue ''
      property[:heating] = response.xpath('//table[@id="Residential"]/tbody/tr[17]/td[2]')[0].inner_text.squeeze(" ").strip rescue ''
      property[:heating_fuel_type] = response.xpath('//table[@id="Residential"]/tbody/tr[18]/td[2]')[0].inner_text.squeeze(" ").strip rescue ''
      property[:heating_system] = response.xpath('//table[@id="Residential"]/tbody/tr[19]/td[2]')[0].inner_text.squeeze(" ").strip rescue ''
      property[:attic_code] = response.xpath('//table[@id="Residential"]/tbody/tr[20]/td[2]')[0].inner_text.squeeze(" ").strip rescue ''
      property[:fireplaces] = response.xpath('//table[@id="Residential"]/tbody/tr[21]/td[2]')[0].inner_text.squeeze(" ").strip rescue ''
      property[:parking] = response.xpath('//table[@id="Residential"]/tbody/tr[22]/td[2]')[0].inner_text.squeeze(" ").strip rescue ''
      property[:total_rooms] = response.xpath('//table[@id="Residential"]/tbody/tr[24]/td[2]')[0].inner_text.squeeze(" ").strip rescue ''
      property[:full_baths] = response.xpath('//table[@id="Residential"]/tbody/tr[25]/td[2]')[0].inner_text.squeeze(" ").strip rescue ''
      property[:half_baths] = response.xpath('//table[@id="Residential"]/tbody/tr[26]/td[2]')[0].inner_text.squeeze(" ").strip rescue ''
      property[:total_fixtures] = response.xpath('//table[@id="Residential"]/tbody/tr[27]/td[2]')[0].inner_text.squeeze(" ").strip rescue ''
      property[:additional_fixtures] = response.xpath('//table[@id="Residential"]/tbody/tr[28]/td[2]')[0].inner_text.squeeze(" ").strip rescue ''
      property[:bedrooms] = response.xpath('//table[@id="Residential"]/tbody/tr[29]/td[2]')[0].inner_text.squeeze(" ").strip rescue ''
      property[:family_room] = response.xpath('//table[@id="Residential"]/tbody/tr[30]/td[2]')[0].inner_text.squeeze(" ").strip rescue ''
      property[:living_units] = response.xpath('//table[@id="Residential"]/tbody/tr[31]/td[2]')[0].inner_text.squeeze(" ").strip rescue ''
      unless property[:parid].blank?
        CSV.open(DATA_FILE, "a+") do |csv|
          now = Time.now.utc
          csv << [ property[:parid], property[:site_location], property[:map_number], 
                  property[:municipality], property[:school_district], property[:property_type], 
                  property[:owner1], property[:owner2],
                  property[:last_sale_date], property[:last_sale_price], property[:type_of_assessment], 
                  property[:assessment_value], property[:assessment_date],property[:assessment_reason_for_change], 
                  property[:assessment_comment],
                  property[:grade], property[:cdu], property[:style], property[:acres], property[:year_built], property[:remodeled_year],
                  property[:base_area], property[:finished_basement_area], property[:number_of_stories],
                  property[:exterior_wall], property[:basement], property[:physical_condition], property[:heating],
                  property[:heating_fuel_type], property[:heating_system], property[:attic_code], property[:fireplaces],
                  property[:parking], property[:total_rooms], property[:full_baths], property[:half_baths],
                  property[:total_fixtures], property[:additional_fixtures], property[:bedrooms],
                  property[:family_room], property[:living_units],
                  now, now ]
        end
      end
    end
  end
end

DATA_FILE = 'data/reassessments_by_municipality/swarthmore_reassessment_data.csv'
parids = []
if !File.exist?(DATA_FILE)
  CSV.open(DATA_FILE, "w") do |csv|
    csv << %w(parid site_location map_number municipality school_district property_type owner1 owner2 last_sale_date
              last_sale_price type_of_assessment assessment_value assessment_date assessment_reason_for_change
              assessment_comment grade cdu style acres year_built remodeled_year base_area finished_basement_area
              number_of_stories exterior_wall basement physical_condition heating heating_fuel_type heating_system
              attic_code fireplaces parking total_rooms full_baths half_baths total_fixtures additional_fixtures
              bedrooms family_room living_units created_at updated_at)
  end
else
  CSV.foreach(DATA_FILE) do |row| 
    parids << row[0] if row[0] =~ /\d+/
  end
end

# # Query database and get parcel ids for municipality that are not already in file
properties = PropertyReassessment.where(taxdist: '43').where.not(parid: parids)

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

PropertySearchSpider.crawl!