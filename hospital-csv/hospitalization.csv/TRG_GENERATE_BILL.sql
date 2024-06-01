--------------------------------------------------------
--  DDL for Trigger TRG_GENERATE_BILL
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE TRIGGER "C##ABHI"."TRG_GENERATE_BILL" 
AFTER UPDATE OF discharge_date ON hospitalization
FOR EACH ROW
DECLARE
    v_room_cost    NUMBER;
    v_test_cost    NUMBER;
    v_other_charges  NUMBER;
    v_total_cost   NUMBER;
BEGIN
    -- Check if the discharge date has been updated
    IF :OLD.discharge_date IS NULL AND :NEW.discharge_date IS NOT NULL THEN
        -- Calculate the room cost for the associated hospitalization
        SELECT NVL(SUM(room_cost), 0)
        INTO v_room_cost
        FROM room
        WHERE idroom = :NEW.room_idroom;

        -- Calculate the test cost for the associated hospitalization
        SELECT NVL(SUM(test_cost), 0)
        INTO v_test_cost
        FROM lab_screening
        WHERE episode_idepisode = :NEW.idepisode;

        -- Calculate the other charges for prescriptions for the associated hospitalization
        SELECT NVL(SUM(m_cost * dosage), 0)
        INTO v_other_charges
        FROM prescription p
        JOIN medicine m ON p.idmedicine = m.idmedicine
        WHERE p.idepisode = :NEW.idepisode;

        -- Calculate the total cost of the bill for the associated episode
        v_total_cost := v_room_cost + v_test_cost + v_other_charges;

        -- Insert the bill with the total cost for the associated episode
        INSERT INTO bill (idepisode, room_cost, test_cost, other_charges, total, payment_status, registered_at)
        VALUES (:NEW.idepisode, v_room_cost, v_test_cost, v_other_charges, v_total_cost, 'PENDING', SYSDATE);

    END IF;
END;

/
ALTER TRIGGER "C##ABHI"."TRG_GENERATE_BILL" ENABLE;
