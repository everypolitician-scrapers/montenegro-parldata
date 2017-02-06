#!/bin/env ruby
# encoding: utf-8

require 'scraperwiki'
require 'nokogiri'
require 'open-uri'
require 'colorize'
require 'rest-client'
require 'pry'

@API_URL = 'http://api.parldata.eu/me/skupstina/%s'

def noko_q(endpoint, h)
  result = RestClient.get (@API_URL % endpoint), params: h, accept: :xml
  doc = Nokogiri::XML(result)
  doc.remove_namespaces!
  entries = doc.xpath('resource/resource')
  return entries if (np = doc.xpath('.//link[@rel="next"]/@href')).empty?
  return [entries, noko_q(endpoint, h.merge(page: np.text[/page=(\d+)/, 1]))].flatten
end

def overlap(mem, term)
  mS = mem[:start_date].to_s.empty?  ? '0000-00-00' : mem[:start_date]
  mE = mem[:end_date].to_s.empty?    ? '9999-99-99' : mem[:end_date]
  tS = term[:start_date].to_s.empty? ? '0000-00-00' : term[:start_date]
  tE = term[:end_date].to_s.empty?   ? '9999-99-99' : term[:end_date]

  return unless mS < tE && mE > tS
  (s, e) = [mS, mE, tS, tE].sort[1,2]
  return { 
    start_date: s == '0000-00-00' ? nil : s,
    end_date:   e == '9999-99-99' ? nil : e,
  }
end

# http://api.parldata.eu/me/skupstina/organizations?where={"classification":"chamber"}
xml = noko_q('organizations', where: %Q[{"classification":"chamber"}] )
xml.each do |chamber|
  term = { 
    id: chamber.xpath('.//identifiers[scheme[text()="skupstina.me/chamber"]]/identifier').text,
    identifier__parldata: chamber.xpath('.//id').text,
    name: chamber.xpath('.//name').text,
  }
  (term[:start_date], term[:end_date]) = term[:name].split(/\s/).find_all { |t| t[/\d{4}/] }
  puts term
  ScraperWiki.save_sqlite([:id], term, 'terms')

  # http://api.parldata.eu/me/skupstina/memberships?where={"organization_id":"550a7f50273a3965683bd824"}&embed=["person.memberships.organization"]
  mems = noko_q('memberships', { 
    where: %Q[{"organization_id":"#{term[:identifier__parldata]}"}],
    max_results: 50,
    embed: '["person.memberships.organization"]'
  })

  mems.each do |mem|
    person = mem.xpath('person') or next
    data = { 
      id: person.xpath('id').text,
      identifier__skupstina: person.xpath('.//identifiers[scheme[text()="skupstina.me/people"]]/identifier').text,
      birth_date: person.xpath('birth_date').text,
      name: person.xpath('name').text,
      image: person.xpath('image').text,
      source: person.xpath('sources/url').first.text,
      term: term[:id],
    }
    if data[:name].to_s.empty?
      old = mem.xpath('.//changes/property[.="name"]/..').sort_by { |n| n.xpath('end_date').text }.last
      if old
        data[:name] = old.xpath('value').text
        warn "#{data[:id]} has no name: rescuing '#{data[:name]}' from changes"
      else
        warn "#{data[:id]} has no name"
        next
      end
    end

    mems = person.xpath('memberships[organization[classification[text()="party"]]]').map { |m|
      {
        party: m.xpath('organization/name').text,
        party_id: m.xpath('.//identifiers[scheme[text()="skupstina.me/parties"]]/identifier').text,
        start_date: m.xpath('start_date').text,
        end_date: m.xpath('end_date').text,
      }
    }.select { |m| overlap(m, term) } 

    if mems.count.zero?
      row = data.merge({
        party: 'Unknown', # or none?
        party_id: '_unknown',
      })
      ScraperWiki.save_sqlite([:id, :term], row)
    else
      mems.each do |mem|
        range = overlap(mem, term) or raise "No overlap"
        row = data.merge(mem).merge(range)
        ScraperWiki.save_sqlite([:id, :term, :start_date], row)
      end
    end
  end
end

