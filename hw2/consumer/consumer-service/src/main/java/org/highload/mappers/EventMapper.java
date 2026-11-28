package org.highload.mappers;

import org.highload.dto.EventDto;
import org.highload.entities.Event;
import org.mapstruct.Mapper;
import org.mapstruct.MappingConstants;

@Mapper(componentModel = MappingConstants.ComponentModel.SPRING)
public interface EventMapper {

    Event mapDtoToEntity(EventDto dto);

}
