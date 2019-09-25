# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

import re
from scrapy.exceptions import DropItem
from jedeschule.items import School

# School( name=item.get('name'),
#         id=item.get('id'),
#         address=item.get('address'),
#         website=item.get('website'),
#         email=item.get('email'),
#         school_type=item.get('schultyp'),
#         legal_status=item.get('legal_status'),
#         provider=item.get('provider'),
#         fax=item.get('fax'),
#         phone=item.get('telephone'),
#         director=item.get('telephone'))


def deobfuscate_th_email(orig):
    """
    Reverse-engineered version of the deobfuscation code on the website.

    :param orig: the obfuscated string or the whole function call (`$(function() {...})`),
        as long as it contains the prefix `#3b` and the suffix `3e#`.
    :return: the deofuscated string
    """
    orig = re.search(r'#3b[a-z0-9 ]+3e#', orig).group(0)
    s = orig.replace(' ', '').replace('#3b', '').replace('3e#', '').replace('o', '')

    result = ''
    last_value = 0
    current_value = 0
    for i, c in enumerate(s):
        if c.isnumeric():
            current_value = int(c)
        else:
            current_value = ord(c)-97+10

        if i % 2 == 1:
            t = int(last_value * 23 + current_value) // 2
            result += chr(t)
        last_value = current_value

    return result


class SchoolPipeline(object):
    def process_item(self, item, spider):
        if spider.name == 'saarland':
            address = u"{} {}".format(item.get('street', ""), item.get('zip', ""))
            if item.get('email'):
                email = item['email'].replace('mailto:', '').replace('%40', '@')
            else:
                email = None
            school = School(name=item.get('name'),
                            phone=item.get('telephone'),
                            director=item.get('telephone'),
                            website=item.get('website'),
                            fax=item.get('fax'),
                            email=email,
                            address=address)
        elif spider.name == 'niedersachsen':
            address = u"{} {}".format(item.get('Straße', ""), item.get('Ort', ""))
            school = School(name=item.get('Schule'),
                            phone=item.get('Tel'),
                            email=item.get('E-Mail'),
                            website=item.get('Homepage'),
                            address=address,
                            id='NDS-{}'.format(item.get('Schulnummer')))
        elif spider.name == 'bayern':
            school = School(name=item.get('Name'),
                            phone=item.get('Telefon'),
                            website=item.get('website'),
                            address=item.get('Anschrift'),
                            id='BAY-{}'.format(item.get('Schulnummer')))
        elif spider.name == 'thueringen':
            city_parts = item.get('Ort').split()
            zip, city = city_parts[0], ' '.join(city_parts[1:])
            school = School(name=item.get('Schulname'),
                            id='TH-{}'.format(item.get('Schulnummer')),
                            address=u"{}".format(item.get('Straße')),
                            zip=zip,
                            city=city,
                            website=item.get('Internet'),
                            email=deobfuscate_th_email(item.get('E-Mail')),
                            school_type=item.get('Schulart'),
                            provider=item.get('Schulträger'),
                            fax=item.get('Telefax'),
                            phone=item.get('Telefon'))
        elif spider.name == 'schleswig-holstein':
            school = School(name=item.get('Name'),
                            id='SH-{}'.format(item.get('Dienststellen Nr.')),
                            address=u"{} {} {}".format(item.get('Straße'), item.get("PLZ"), item.get("Ort")),
                            email=item.get('EMail'),
                            school_type=item.get('Organisationsform'),
                            legal_status=item.get('Rechtsstatus'),
                            provider=item.get('Träger'),
                            fax=item.get('Fax'),
                            phone=item.get('Telefon'),
                            director=item.get('Schulleiter(-in)'))
        elif spider.name == 'bremen':
            ansprechpersonen = item['Ansprechperson'].replace('Schulleitung:', '').replace('Vertretung:', ',').split(',')
            item['Schulleitung'] = ansprechpersonen[0]
            item['Vertretung'] = ansprechpersonen[1]
            school = School(name=item.get('name'),
                            address=item.get('Anschrift:'),
                            website=item.get('Internet'),
                            email=item.get('E-Mail-Adresse'),
                            fax=item.get('Telefax'),
                            phone=item.get('Telefon'))
        elif spider.name == 'sachsen':
            school = School(name=item.get('title'),
                            id='SN-{}'.format(item.get('Dienststellenschlüssel')),
                            address=item.get('Postanschrift'),
                            website=item.get('Homepage'),
                            email=item.get('E-Mail'),
                            school_type=item.get('Einrichtungsart'),
                            legal_status=item.get('Rechtsstellung'),
                            provider=item.get('Schulträger'),
                            fax=item.get('Telefax'),
                            phone=item.get('phone_numbers'),
                            director=item.get('Schulleiter'))
        elif spider.name == 'sachsen-anhalt':
            school = School(name=item.get('Name'),
                            address=item.get('Addresse'),
                            website=item.get('Homepage'),
                            email=item.get('E-Mail'),
                            fax=item.get('Fax'),
                            phone=item.get('Telefon')
                            )
        elif spider.name == 'brandenburg':
            school = School(
                name=item.get('name'),
                id='BB-{}'.format(item.get('nummer')),
                address=item.get('Adresse'),
                website=item.get('Internet'),
                email=item.get('E-Mail'),
                school_type=item.get('Schulform'),
                provider=item.get('Schulamt'),
                fax=item.get('Fax'),
                phone=item.get('Telefon'),
                director=item.get('Schulleiter/in'))
        elif spider.name == 'rheinland-pfalz':
            school = School(name=item.get('name'),
                            id='RP-{}'.format(item.get('id')),
                            address=item.get('Adresse'),
                            website=item.get('Internet'),
                            email=item.get('E-Mail'),
                            school_type=item.get('Schulform'),
                            fax=item.get('Fax'),
                            phone=item.get('Telefon'))
        else:
            return item
            raise DropItem("Missing name in %s" % item)
        return {'info': school, 'item': item}
