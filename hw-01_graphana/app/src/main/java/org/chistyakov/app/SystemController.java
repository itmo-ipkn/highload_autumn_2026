package org.chistyakov.app;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class SystemController {
    @GetMapping("/health")
    public ResponseEntity<Void> heath() {
        return ResponseEntity.ok().build();
    }
}
