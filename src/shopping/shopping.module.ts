import { Module } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { ShoppingService } from './shopping.service';
import { ShoppingController } from './shopping.controller';
import { ShoppingEntity } from './entities/shopping.entity';


@Module({
  imports: [TypeOrmModule.forFeature([ShoppingEntity])],
  controllers: [ShoppingController],
  providers: [ShoppingService],
})
export class ShoppingModule {}