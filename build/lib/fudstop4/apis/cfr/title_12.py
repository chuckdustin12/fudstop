from xml.etree.ElementTree import ElementTree, Element
from typing import List, Dict
from cfr_helpers import clean_string





def parse_title_12_xml(file_path: str) -> Dict[str, List[Dict]]:
    # Initialize data structures to hold parsed data
    titles = []
    chapters = []
    parts = []
    authorities = []
    sources = []
    
    # Parse the XML file
    tree = ElementTree()
    tree.parse(file_path)
    
    title_id = 1  # Initialize title_id
    chapter_id = 1  # Initialize chapter_id
    part_id = 1  # Initialize part_id
    
    for div1 in tree.findall(".//DIV1"):
        title_name = div1.find(".//HEAD").text if div1.find(".//HEAD") is not None else ""
        title_name = clean_string(title_name)
        titles.append({"id": title_id, "title_name": title_name})
        
        for div3 in div1.findall(".//DIV3"):
            chapter_name = div3.find(".//HEAD").text if div3.find(".//HEAD") is not None else ""
            chapter_name = clean_string(chapter_name)  # Clean the string here
            chapters.append({"id": chapter_id, "chapter_name": chapter_name, "title_id": title_id})
            
            
            for div5 in div3.findall(".//DIV5"):
                part_name = div5.find(".//HEAD").text if div5.find(".//HEAD") is not None else ""
                part_name = clean_string(part_name)
                parts.append({"id": part_id, "part_name": part_name, "chapter_id": chapter_id})
                
                authority = div5.find(".//AUTH")
                if authority is not None:
                    authority_text = "".join(authority.itertext())
                    authority_text = clean_string(authority_text)
                    authorities.append({"id": len(authorities) + 1, "authority_text": authority_text, "part_id": part_id})
                
                source = div5.find(".//SOURCE")
                if source is not None:
                    source_text = "".join(source.itertext())
                    source_text = clean_string(source_text)
                    sources.append({"id": len(sources) + 1, "source_text": source_text, "part_id": part_id})
                
                part_id += 1  # Increment part_id for the next part
            chapter_id += 1  # Increment chapter_id for the next chapter
        title_id += 1  # Increment title_id for the next title
    
    return {
        "titles": titles,
        "chapters": chapters,
        "parts": parts,
        "authorities": authorities,
        "sources": sources
    }