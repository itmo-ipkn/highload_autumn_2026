package org.highload.services;

import lombok.RequiredArgsConstructor;
import org.highload.dto.ItemDto;
import org.highload.dto.ItemRequestDto;
import org.highload.entities.Item;
import org.highload.exceptions.InternalServerException;
import org.highload.exceptions.NotFoundException;
import org.highload.mappers.ItemMapper;
import org.highload.properties.InternalErrorsProperty;
import org.highload.repositories.ItemRepository;
import org.jspecify.annotations.NonNull;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class ItemService {

    private final InternalErrorsProperty internalErrorsProperty;
    private final ItemRepository itemRepository;
    private final ItemMapper itemMapper;

    public Long create(@NonNull ItemRequestDto requestDto) {
        checkInternalError();
        Item item = itemMapper.mapRequestDtoToEntity(requestDto);
        return itemRepository.save(item);
    }

    public ItemDto getById(@NonNull Long id) {
        checkInternalError();
        Item item = itemRepository.findById(id)
                .orElseThrow(() -> new NotFoundException("Not found item by id: %s".formatted(id)));
        return itemMapper.mapEntityToDto(item);
    }

    public ItemDto updateById(@NonNull Long id, @NonNull ItemRequestDto requestDto) {
        checkInternalError();
        Item item = itemMapper.mapRequestDtoToEntity(requestDto);
        Item updatedItem = itemRepository.updateById(id, item)
                .orElseThrow(() -> new NotFoundException("Not found item by id: %s".formatted(id)));

        return itemMapper.mapEntityToDto(updatedItem);
    }

    private void checkInternalError() {
        double random = Math.random();
        if (random < internalErrorsProperty.getProbabilityInternalError()) {
            throw new InternalServerException("Internal error");
        }
    }

}
