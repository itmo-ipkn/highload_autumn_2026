package org.highload.mappers;

import org.highload.dto.ItemDto;
import org.highload.dto.ItemRequestDto;
import org.highload.entities.Item;
import org.mapstruct.Mapper;
import org.mapstruct.MappingConstants;

@Mapper(componentModel = MappingConstants.ComponentModel.SPRING)
public interface ItemMapper {

    Item mapRequestDtoToEntity(ItemRequestDto dto);

    ItemDto mapEntityToDto(Item item);

}
